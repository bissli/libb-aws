import pytest

import aws.base


@pytest.fixture(autouse=True)
def clean_settings():
    """Reset module settings between tests."""
    original = aws.base._settings.copy()
    aws.base._settings.clear()
    yield
    aws.base._settings.clear()
    aws.base._settings.update(original)
