# libb-aws

Lightweight wrapper around boto3 for S3 operations, with optional
Google Drive transfer support for large-scale migrations.

## Install

```bash
pip install libb-aws

# With Google Drive transfer support
pip install libb-aws[google]
```

## Setup

```python
import aws

aws.configure(
    region='us-east-1',
    access_key='AKIAIOSFODNN7EXAMPLE',
    secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    tmpdir='/tmp/aws',
)
```

## S3 Operations

```python
ctx = aws.S3Context()

# List buckets
ctx.list_buckets()

# List files in a bucket
for obj in ctx.list_files('my-bucket', prefix='data/'):
    print(obj.key)

# Upload (file path or file-like object)
ctx.upload_file('/path/to/file.txt', 'my-bucket', 'data/file.txt')

# Download
ctx.download_file('my-bucket', 'data/file.txt')

# Check existence
ctx.exists('my-bucket', 'data/file.txt')

# Rename
ctx.rename_file('my-bucket', 'data/old.txt', 'data/new.txt')

# Delete
ctx.delete_file('my-bucket', 'data/file.txt')
```

## Google Drive to S3 Transfer

Transfer files from Google Drive directly into S3 with concurrent
workers. Requires the `google` extra and `goog.configure()` to be
called first.

```python
import goog
import aws

goog.configure(
    account='service@example.com',
    rootid={'SharedDrive': 'abc123'},
    app_configs={'drive': {
        'key': '/path/to/credentials.json',
        'scopes': ['https://www.googleapis.com/auth/drive'],
        'version': 'v3',
    }},
)
aws.configure(region='us-east-1', access_key='...', secret_key='...')

s3 = aws.S3Context()

# Transfer an entire folder tree
result = aws.transfer_google_tree(
    s3, '/SharedDrive/Data', 'my-bucket',
    prefix='archive/',
    workers=4,
    skip_existing=True,
)
print(result)
# {'transferred': 150, 'skipped': 10, 'failed': []}
```

### Single file transfer

```python
from goog.drive import Drive

drive = Drive()
entry = {'path': '/SharedDrive/report.pdf', 'id': '...', 'name': 'report.pdf',
         'mimeType': 'application/pdf'}

status = aws.transfer_google_file(drive, s3, entry, 'my-bucket', prefix='docs/')
# 'transferred', 'exported', or 'skipped'
```

### How it works

- Files are downloaded via `drive.read()` into a seekable `BytesIO`,
  then uploaded via `s3.upload_file()`. The seekable buffer lets boto3
  use parallel multipart uploads for large files.
- Google Workspace files (Docs, Sheets, Slides) are automatically
  detected and exported to portable formats (.txt, .csv, .pdf).
- Each worker thread gets its own Drive and S3 instance for thread
  safety.
- `skip_existing=True` checks S3 before downloading, enabling safe
  re-runs of interrupted migrations.
- Rate limit errors abort the current worker's chunk; other errors are
  logged and skipped. Re-run to retry failures.
