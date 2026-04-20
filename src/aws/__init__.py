"""AWS utilities for S3.
"""
from aws.base import configure, get_settings
from aws.s3 import S3Context

__all__ = [
    'configure',
    'get_settings',
    'S3Context',
    ]
