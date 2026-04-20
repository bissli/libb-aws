import logging
import os
import random
import string
from pathlib import Path

import boto3
import botocore
from aws.base import get_settings

logger = logging.getLogger(__name__)


class S3Context:
    """Wrapper for boto3 S3 access"""

    def __init__(self, region=None, access_key=None, secret_key=None):
        settings = get_settings()
        self.region = region or settings.get('region')
        session = boto3.Session(
            aws_access_key_id=access_key or settings.get('access_key'),
            aws_secret_access_key=secret_key or settings.get('secret_key'),
            region_name=self.region,
        )
        self.s3 = session.resource('s3')
        self.client = session.client('s3')

    def list_buckets(self):
        """Get all the buckets within the region"""
        return tuple(self.s3.buckets.all())

    def list_files(self, bucket, prefix=''):
        """Get all the files without a bucket.

        For subdirectories, use a / delimiter. For example bucket Foo,
        with directory Foo/bar/baz: bucket=Foo, prefix=bar/baz
        """
        if prefix:
            logger.info(f'Filtering to objects in directory {prefix}')
        yield from self.s3.Bucket(bucket).objects.filter(Prefix=prefix)

    def create_buckets(self, bucket_name=None, repeat=1):
        """Create buckets with random name for the given regions list with ACL private."""
        for idx in range(repeat):
            try:
                name = (
                    bucket_name
                    if bucket_name is not None
                    else ''.join(
                        random.choice(string.ascii_letters) for _ in range(15)
                    ).lower()
                )
                kw_args = {
                    'Bucket': name,
                    'ACL': 'private',
                    'CreateBucketConfiguration': {'LocationConstraint': self.region},
                }
                bucket = self.client.create_bucket(**kw_args)
                if bucket['ResponseMetadata']['HTTPStatusCode'] == 200:
                    logger.info(f'Created new bucket {name} at region {self.region}')
                else:
                    logger.error(f'Can not create a new bucket of name {name}')
            except botocore.exceptions.ClientError as e:
                logger.error(f'{e}')

    def upload_file(self, fileobj, bucket, key):
        """Upload a file in S3 bucket.
        - Ex: bucket Foo, folder in bucket Foo/Bar/, filepath /tmp/Baz.txt
            - fileobj = /tmp/Baz.txt (as a path)
            - bucket = Foo
            - key = Bar/Baz.txt (no pre-slash!)
        """
        key = key.removeprefix('/')
        opened = False
        try:
            if isinstance(fileobj, str):
                fileobj = Path(fileobj).open('rb')
                opened = True
            self.s3.meta.client.upload_fileobj(fileobj, bucket, key)
            logger.info(f'Uploaded {key} to {bucket}')
        except Exception as exc:
            logger.error(exc)
            raise
        finally:
            if opened:
                fileobj.close()

    def rename_file(self, bucket, key_old, key_new):
        """Rename an existing file in S3 bucket.
        - Ex: bucket Foo, folder in bucket Foo/Bar/, filepath /tmp/Baz.txt
            - bucket = Foo
            - key_old = Bar/Baz.txt (no pre-slash!)
            - key_new = Bar/Fizz.txt (no pre-slash!)
        """
        try:
            self.s3.Object(bucket, key_new).copy_from(CopySource=f'{bucket}/{key_old}')
            self.delete_file(bucket, key_old)
        except Exception as exc:
            logger.error(exc)
            raise

    def delete_file(self, bucket, key):
        """Delete a file in S3 bucket."""
        try:
            logger.info(f'Deleting {key} from bucket {bucket}')
            self.s3.Object(bucket, key).delete()
            logger.info(f'Successfully deleted {key}')
        except botocore.exceptions.ClientError as e:
            logger.error(f'Deletion of {key} failed')
            logger.error(e)
            raise

    def exists(self, bucket: str, key: str) -> bool:
        """Check if a file exists in S3 bucket.
        """
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def download_file(self, bucket, key, savedir=None, saveas=''):
        """Download a file from S3 bucket
        - Ex: bucket Foo, file path Foo/Bar/Baz.txt
            - bucket = Foo
            - key = Bar/Baz.txt (no pre-slash!)
        - Can rename the downloaded file with saveas, Ex Baz1.txt
        """
        try:
            key = key.removeprefix('/')
            if savedir is None:
                savedir = get_settings().get('tmpdir', '.')
            saveto = os.path.join(Path(savedir).resolve(), key)
            Path(saveto).parent.mkdir(parents=True, exist_ok=True)
            if saveas:
                saveto, _ = os.path.split(saveto)
                saveto = os.path.join(saveto, saveas)
            with Path(saveto).open('wb') as f:
                self.client.download_fileobj(bucket, key, f)
            logger.info(f'Downloaded {bucket}/{key}')
            return saveto
        except botocore.exceptions.ClientError as ce:
            if ce.response['Error']['Code'] == '404':
                logger.error('The file does not exist')
            else:
                logger.error('Download failed')
            raise

    def download_all(self, bucket, savedir=None):
        """Download all files in a bucket"""
        if savedir is None:
            savedir = get_settings().get('tmpdir', '.')
        for obj in self.s3.Bucket(bucket).objects.all():
            saveto = os.path.join(Path(savedir).resolve(), str(obj.key))
            saveto, filename = os.path.split(saveto)
            saveto = os.path.join(saveto, '')
            if not Path(saveto).exists():
                Path(saveto).mkdir(parents=True)
            try:
                if filename:
                    self.download_file(bucket, obj.key, savedir)
            except Exception as exc:
                logger.error(f'Unable to download {saveto}{str(obj.key)}')
                raise
