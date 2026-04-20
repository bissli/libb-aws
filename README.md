# libb-aws

Lightweight wrapper around boto3 for S3 operations.

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

## Usage

```python
ctx = aws.S3Context()

# List buckets
ctx.list_buckets()

# List files in a bucket
for obj in ctx.list_files('my-bucket', prefix='data/'):
    print(obj.key)

# Upload
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
