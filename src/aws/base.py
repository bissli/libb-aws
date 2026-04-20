"""Base configuration for AWS module.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)

_settings = {}


def configure(
    region: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    tmpdir: str | None = None,
) -> None:
    """Configure module defaults.

    This should be called once at application startup to set default values
    that will be used across all AWS clients.

    Example:
        >>> import aws
        >>> aws.configure(
        ...     region='us-east-1',
        ...     access_key='AKIAIOSFODNN7EXAMPLE',
        ...     secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        ...     tmpdir='/tmp/aws',
        ... )
    """
    if region is not None:
        _settings['region'] = region
    if access_key is not None:
        _settings['access_key'] = access_key
    if secret_key is not None:
        _settings['secret_key'] = secret_key
    if tmpdir is not None:
        _settings['tmpdir'] = tmpdir


def get_settings() -> dict[str, Any]:
    """Get current module settings.
    """
    return _settings
