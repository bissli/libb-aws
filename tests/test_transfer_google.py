"""Tests for Google Drive to S3 transfer module."""
import io
from unittest.mock import MagicMock, patch

import pytest
from aws.transfer_google import _process_chunk, _transfer_one
from aws.transfer_google import transfer_google_file, transfer_google_tree


@pytest.fixture
def mock_drive():
    """Fake Drive instance with read/export/walk stubs."""
    drive = MagicMock()
    drive.read.return_value = io.BytesIO(b'file-content')
    drive.export.return_value = io.BytesIO(b'exported-content')
    drive.walk.return_value = iter([])
    return drive


@pytest.fixture
def mock_s3():
    """Fake S3Context instance."""
    s3 = MagicMock()
    s3.exists.return_value = False
    return s3


def _entry(path='/Root/folder/test.pdf', mime='application/pdf'):
    """Build a walk detail dict."""
    return {
        'path': path,
        'id': 'fake-id',
        'name': path.rsplit('/', 1)[-1],
        'mimeType': mime,
        }


class TestTransferGoogleFile:

    def test_regular_file(self, mock_drive, mock_s3):
        """Regular file downloaded via read() and uploaded to S3."""
        entry = _entry('/Root/data/report.pdf')
        status = transfer_google_file(
            mock_drive, mock_s3, entry, 'bucket', prefix='archive/')
        assert status == 'transferred'
        mock_drive.read.assert_called_once_with(
            folder='/Root/data', filename='report.pdf')
        mock_s3.upload_file.assert_called_once()
        call_args = mock_s3.upload_file.call_args
        assert call_args[0][1] == 'bucket'
        assert call_args[0][2] == 'archive/Root/data/report.pdf'

    def test_workspace_doc_exported(self, mock_drive, mock_s3):
        """Google Doc exported as .docx via export()."""
        entry = _entry(
            '/Root/notes/Meeting Notes',
            'application/vnd.google-apps.document')
        status = transfer_google_file(
            mock_drive, mock_s3, entry, 'bucket')
        assert status == 'exported'
        mock_drive.export.assert_called_once_with(
            folder='/Root/notes', filename='Meeting Notes',
            mime_type=(
                'application/vnd.openxmlformats-officedocument'
                '.wordprocessingml.document'))
        key = mock_s3.upload_file.call_args[0][2]
        assert key.endswith('.docx')

    def test_workspace_sheet_exported(self, mock_drive, mock_s3):
        """Google Sheet exported as .csv."""
        entry = _entry(
            '/Root/Budget',
            'application/vnd.google-apps.spreadsheet')
        status = transfer_google_file(
            mock_drive, mock_s3, entry, 'bucket')
        assert status == 'exported'
        mock_drive.export.assert_called_once_with(
            folder='/Root', filename='Budget',
            mime_type=(
                'application/vnd.openxmlformats-officedocument'
                '.spreadsheetml.sheet'))
        key = mock_s3.upload_file.call_args[0][2]
        assert key.endswith('.xlsx')

    def test_unsupported_workspace_type_skipped(self, mock_drive, mock_s3):
        """Unmapped Workspace mimeType logged and skipped."""
        entry = _entry(
            '/Root/form',
            'application/vnd.google-apps.form')
        status = transfer_google_file(
            mock_drive, mock_s3, entry, 'bucket')
        assert status == 'skipped'
        mock_drive.read.assert_not_called()
        mock_drive.export.assert_not_called()
        mock_s3.upload_file.assert_not_called()

    def test_skip_existing(self, mock_drive, mock_s3):
        """File already in S3 skipped when skip_existing=True."""
        mock_s3.exists.return_value = True
        entry = _entry()
        status = transfer_google_file(
            mock_drive, mock_s3, entry, 'bucket')
        assert status == 'skipped'
        mock_drive.read.assert_not_called()

    def test_skip_existing_disabled(self, mock_drive, mock_s3):
        """File transferred even if in S3 when skip_existing=False."""
        mock_s3.exists.return_value = True
        entry = _entry()
        status = transfer_google_file(
            mock_drive, mock_s3, entry, 'bucket',
            skip_existing=False)
        assert status == 'transferred'
        mock_drive.read.assert_called_once()

    def test_prefix_applied(self, mock_drive, mock_s3):
        """Prefix prepended to S3 key."""
        entry = _entry('/Root/file.txt', 'text/plain')
        transfer_google_file(
            mock_drive, mock_s3, entry, 'bucket', prefix='backup/')
        key = mock_s3.upload_file.call_args[0][2]
        assert key == 'backup/Root/file.txt'


class TestTransferOne:

    def test_success_passthrough(self, mock_drive, mock_s3):
        """Successful transfer returns status."""
        entry = _entry()
        status = _transfer_one(
            mock_drive, mock_s3, entry, 'bucket', '', True)
        assert status == 'transferred'

    def test_rate_limit_promotes_to_rate_limit_error(
        self, mock_drive, mock_s3,
    ):
        """HttpError with rate limit promotes to RateLimitError."""
        from goog.base import RateLimitError

        resp = MagicMock()
        resp.status = 429
        exc = MagicMock(spec=Exception)
        exc.resp = resp
        exc.__class__ = type(
            'HttpError', (Exception,), {'resp': resp})
        mock_drive.read.side_effect = type(
            'HttpError', (Exception,), {'resp': resp})()

        with patch('aws.transfer_google.is_rate_limit', return_value=True):
            with patch(
                'aws.transfer_google.HttpError',
                type(mock_drive.read.side_effect),
            ):
                with pytest.raises(RateLimitError):
                    _transfer_one(
                        mock_drive, mock_s3, entry=_entry(),
                        bucket='b', prefix='', skip_existing=True)

    def test_non_rate_limit_error_propagates(self, mock_drive, mock_s3):
        """Non-rate-limit exceptions propagate unchanged."""
        mock_drive.read.side_effect = ValueError('bad file')
        with pytest.raises(ValueError, match='bad file'):
            _transfer_one(
                mock_drive, mock_s3, entry=_entry(),
                bucket='b', prefix='', skip_existing=True)


class TestProcessChunk:

    @patch('aws.transfer_google.S3Context')
    @patch('aws.transfer_google.Drive')
    def test_processes_all_files(self, mock_drive_cls, mock_s3_cls):
        """All files in chunk processed and counted."""
        mock_drive = MagicMock()
        mock_drive.read.return_value = io.BytesIO(b'data')
        mock_drive_cls.return_value = mock_drive
        mock_s3 = MagicMock()
        mock_s3.exists.return_value = False
        mock_s3_cls.return_value = mock_s3

        entries = [_entry(f'/Root/file{i}.pdf') for i in range(3)]
        pbar = MagicMock()

        transferred, skipped, failed = _process_chunk(
            entries, 'bucket', '', True, pbar)

        assert transferred == 3
        assert skipped == 0
        assert failed == []
        assert pbar.update.call_count == 3

    @patch('aws.transfer_google.S3Context')
    @patch('aws.transfer_google.Drive')
    def test_rate_limit_aborts_chunk(self, mock_drive_cls, mock_s3_cls):
        """RateLimitError aborts remaining files in chunk."""
        from goog.base import RateLimitError

        mock_drive = MagicMock()
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RateLimitError('rate limited')
            return io.BytesIO(b'data')

        mock_drive.read.side_effect = side_effect
        mock_drive_cls.return_value = mock_drive
        mock_s3 = MagicMock()
        mock_s3.exists.return_value = False
        mock_s3_cls.return_value = mock_s3

        entries = [_entry(f'/Root/file{i}.pdf') for i in range(5)]
        pbar = MagicMock()

        transferred, skipped, failed = _process_chunk(
            entries, 'bucket', '', True, pbar)

        assert transferred == 1
        assert len(failed) == 1
        assert pbar.update.call_count == 1

    @patch('aws.transfer_google.S3Context')
    @patch('aws.transfer_google.Drive')
    def test_other_errors_logged_and_continued(
        self, mock_drive_cls, mock_s3_cls,
    ):
        """Non-rate-limit errors logged, remaining files continue."""
        mock_drive = MagicMock()
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise ValueError('corrupt file')
            return io.BytesIO(b'data')

        mock_drive.read.side_effect = side_effect
        mock_drive_cls.return_value = mock_drive
        mock_s3 = MagicMock()
        mock_s3.exists.return_value = False
        mock_s3_cls.return_value = mock_s3

        entries = [_entry(f'/Root/file{i}.pdf') for i in range(3)]
        pbar = MagicMock()

        transferred, skipped, failed = _process_chunk(
            entries, 'bucket', '', True, pbar)

        assert transferred == 2
        assert len(failed) == 1
        assert failed[0] == '/Root/file1.pdf'
        assert pbar.update.call_count == 3


class TestTransferGoogleTree:

    @patch('aws.transfer_google._process_chunk')
    @patch('aws.transfer_google.Drive')
    def test_empty_folder(self, mock_drive_cls, mock_process):
        """Empty folder returns zero counts."""
        mock_drive = MagicMock()
        mock_drive.walk.return_value = iter([])
        mock_drive_cls.return_value = mock_drive

        result = transfer_google_tree(
            MagicMock(), '/Root/empty', 'bucket')

        assert result == {
            'transferred': 0, 'skipped': 0, 'failed': []}
        mock_process.assert_not_called()

    @patch('aws.transfer_google._process_chunk')
    @patch('aws.transfer_google.Drive')
    def test_partitions_and_collects(self, mock_drive_cls, mock_process):
        """Files partitioned across workers, results aggregated."""
        entries = [_entry(f'/Root/file{i}.pdf') for i in range(6)]
        mock_drive = MagicMock()
        mock_drive.walk.return_value = iter(entries)
        mock_drive_cls.return_value = mock_drive

        mock_process.return_value = (2, 1, [])

        result = transfer_google_tree(
            MagicMock(), '/Root', 'bucket', workers=3)

        assert mock_process.call_count == 3
        assert result['transferred'] == 6
        assert result['skipped'] == 3

    @patch('aws.transfer_google._process_chunk')
    @patch('aws.transfer_google.Drive')
    def test_fewer_files_than_workers(self, mock_drive_cls, mock_process):
        """Fewer files than workers creates fewer chunks."""
        entries = [_entry(f'/Root/file{i}.pdf') for i in range(2)]
        mock_drive = MagicMock()
        mock_drive.walk.return_value = iter(entries)
        mock_drive_cls.return_value = mock_drive

        mock_process.return_value = (1, 0, [])

        result = transfer_google_tree(
            MagicMock(), '/Root', 'bucket', workers=8)

        assert mock_process.call_count == 2
