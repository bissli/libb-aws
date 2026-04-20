"""Verify module imports without name conflicts or missing dependencies."""
import pathlib


def test_import_aws():
    """Top-level aws package imports cleanly."""
    import aws
    assert hasattr(aws, 'configure')
    assert hasattr(aws, 'get_settings')
    assert hasattr(aws, 'S3Context')


def test_import_base():
    """Base module exports configure and get_settings."""
    from aws.base import configure, get_settings
    assert callable(configure)
    assert callable(get_settings)


def test_import_s3():
    """S3 module exports S3Context."""
    from aws.s3 import S3Context
    assert callable(S3Context)


def test_no_tc_dependency():
    """Module does not depend on the private tc package."""
    import aws.base
    import aws.s3
    for mod in (aws.s3, aws.base):
        source_file = mod.__file__
        contents = pathlib.Path(source_file).read_text()
        assert 'from tc ' not in contents
        assert 'import tc' not in contents


def test_configure_sets_values():
    """configure() stores values retrievable via get_settings()."""
    from aws.base import configure, get_settings
    configure(region='us-west-2', access_key='AK', secret_key='SK', tmpdir='/tmp')
    settings = get_settings()
    assert settings['region'] == 'us-west-2'
    assert settings['access_key'] == 'AK'
    assert settings['secret_key'] == 'SK'
    assert settings['tmpdir'] == '/tmp'


def test_configure_is_additive():
    """configure() only updates non-None values."""
    from aws.base import configure, get_settings
    configure(region='us-east-1')
    configure(tmpdir='/tmp')
    settings = get_settings()
    assert settings['region'] == 'us-east-1'
    assert settings['tmpdir'] == '/tmp'


def test_configure_overwrites():
    """configure() overwrites existing values."""
    from aws.base import configure, get_settings
    configure(region='us-east-1')
    configure(region='eu-west-1')
    assert get_settings()['region'] == 'eu-west-1'


def test_s3context_reads_settings(mocker):
    """S3Context falls back to module settings for credentials."""
    from aws.base import configure
    from aws.s3 import S3Context
    configure(region='us-east-1', access_key='AK', secret_key='SK')
    mock_session = mocker.patch('aws.s3.boto3.Session')
    ctx = S3Context()
    assert ctx.region == 'us-east-1'
    mock_session.assert_called_once_with(
        aws_access_key_id='AK',
        aws_secret_access_key='SK',
        region_name='us-east-1',
    )


def test_s3context_explicit_overrides_settings(mocker):
    """Explicit params to S3Context override module settings."""
    from aws.base import configure
    from aws.s3 import S3Context
    configure(region='us-east-1', access_key='AK', secret_key='SK')
    mock_session = mocker.patch('aws.s3.boto3.Session')
    ctx = S3Context(region='eu-west-1', access_key='AK2', secret_key='SK2')
    assert ctx.region == 'eu-west-1'
    mock_session.assert_called_once_with(
        aws_access_key_id='AK2',
        aws_secret_access_key='SK2',
        region_name='eu-west-1',
    )


def test_all_exports():
    """__all__ matches actual exports."""
    import aws
    for name in aws.__all__:
        assert hasattr(aws, name), f'{name} in __all__ but not importable'
