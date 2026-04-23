"""Google Drive to S3 transfer utilities.

Requires libb-goog: install with ``pip install libb-aws[google]``

Typical usage::

    >>> import goog, aws
    >>> goog.configure(account='svc@example.com', rootid={...}, app_configs={...})
    >>> aws.configure(region='us-east-1', access_key='...', secret_key='...')
    >>> result = aws.transfer_google_tree(
    ...     aws.S3Context(), '/SharedDrive/folder', 'my-bucket',
    ...     prefix='archive/', workers=4)
"""
import logging
import posixpath
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from aws.s3 import S3Context
from goog.base import RateLimitError, is_rate_limit
from goog.drive import FOLDER_MIME, GOOGLE_EXPORT_DEFAULTS, Drive
from googleapiclient.errors import HttpError
from tqdm import tqdm

logger = logging.getLogger(__name__)

WORKSPACE_PREFIX = 'application/vnd.google-apps.'

EXPORT_EXTENSIONS = {
    'text/plain': '.txt',
    'text/csv': '.csv',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': '.pptx',
    'application/pdf': '.pdf',
    'application/vnd.google-apps.script+json': '.json',
    }


def transfer_google_file(
    drive: Drive,
    s3: S3Context,
    entry: dict[str, Any],
    bucket: str,
    prefix: str = '',
    skip_existing: bool = True,
) -> str:
    """Transfer a single file from Google Drive to S3.

    Args:
        drive: Authenticated Drive instance
        s3: Authenticated S3Context instance
        entry: File dict from drive.walk(detail=True) with
            path, id, name, mimeType keys
        bucket: Target S3 bucket name
        prefix: S3 key prefix (e.g. 'archive/')
        skip_existing: Skip files already present in S3

    Returns
        Status string: 'transferred', 'exported', or 'skipped'
    """
    filepath = entry['path']
    folder = posixpath.dirname(filepath)
    filename = posixpath.basename(filepath)
    mime_type = entry['mimeType']

    key = prefix + filepath.lstrip('/')

    is_workspace = (mime_type.startswith(WORKSPACE_PREFIX)
                    and mime_type != FOLDER_MIME)

    if is_workspace:
        export_mime = GOOGLE_EXPORT_DEFAULTS.get(mime_type)
        if not export_mime:
            logger.warning(f'Unsupported Workspace type {mime_type}: {filepath}')
            return 'skipped'
        ext = EXPORT_EXTENSIONS.get(export_mime, '.bin')
        key = posixpath.splitext(key)[0] + ext

    if skip_existing and s3.exists(bucket, key):
        return 'skipped'

    if is_workspace:
        buf = drive.export(folder=folder, filename=filename,
                           mime_type=export_mime)
    else:
        buf = drive.read(folder=folder, filename=filename)

    s3.upload_file(buf, bucket, key)
    return 'exported' if is_workspace else 'transferred'


def _transfer_one(
    drive: Drive,
    s3: S3Context,
    entry: dict[str, Any],
    bucket: str,
    prefix: str,
    skip_existing: bool,
) -> str:
    """Transfer with rate-limit detection.
    """
    try:
        return transfer_google_file(
            drive, s3, entry, bucket, prefix, skip_existing)
    except HttpError as exc:
        if is_rate_limit(exc):
            raise RateLimitError(
                f'Rate limit persisted for {entry["path"]}'
            ) from exc
        raise


def _process_chunk(
    chunk: list[dict],
    bucket: str,
    prefix: str,
    skip_existing: bool,
    pbar: tqdm,
) -> tuple[int, int, list[str]]:
    """Process a list of files with dedicated Drive and S3 instances.
    """
    drive = Drive()
    s3 = S3Context()
    transferred = 0
    skipped = 0
    failed = []
    for entry in chunk:
        try:
            status = _transfer_one(
                drive, s3, entry, bucket, prefix, skip_existing)
            if status == 'skipped':
                skipped += 1
            else:
                transferred += 1
        except RateLimitError:
            tqdm.write(
                f'Rate limit sustained, aborting chunk at {entry["name"]}')
            failed.append(entry['path'])
            break
        except Exception:
            logger.exception(f'Failed to transfer {entry["path"]}')
            failed.append(entry['path'])
        pbar.update(1)
    return transferred, skipped, failed


def transfer_google_tree(
    s3: S3Context,
    folder: str,
    bucket: str,
    prefix: str = '',
    workers: int = 4,
    skip_existing: bool = True,
) -> dict[str, Any]:
    """Transfer a Google Drive folder tree to S3.

    Requires ``goog.configure()`` to have been called beforehand.

    Args:
        s3: Authenticated S3Context (used only for initial validation;
            each worker creates its own instance)
        folder: Google Drive folder path (e.g. '/SharedDrive/Data')
        bucket: Target S3 bucket name
        prefix: S3 key prefix prepended to each file path
        workers: Number of parallel transfer threads
        skip_existing: Skip files already in S3 (enables safe re-runs)

    Returns
        Summary dict with keys: transferred, skipped, failed
    """
    drive = Drive()
    tqdm.write(f'Enumerating files in {folder}...')
    files = list(drive.walk(folder, recursive=True, detail=True))
    tqdm.write(f'Found {len(files)} files to transfer')

    if not files:
        return {'transferred': 0, 'skipped': 0, 'failed': []}

    chunks = [files[i::workers] for i in range(workers)]
    chunks = [c for c in chunks if c]

    total_transferred = 0
    total_skipped = 0
    all_failed = []

    with tqdm(total=len(files), unit='file', desc='Transferring') as pbar:
        with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
            futures = [
                pool.submit(
                    _process_chunk, chunk, bucket, prefix,
                    skip_existing, pbar)
                for chunk in chunks
                ]
            for fut in as_completed(futures):
                transferred, skipped, failed = fut.result()
                total_transferred += transferred
                total_skipped += skipped
                all_failed.extend(failed)

    tqdm.write(
        f'Done: {total_transferred} transferred, '
        f'{total_skipped} skipped, {len(all_failed)} failed')

    return {
        'transferred': total_transferred,
        'skipped': total_skipped,
        'failed': all_failed,
        }
