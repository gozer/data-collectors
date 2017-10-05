from collectors.main import setup_logging


def test_logging_setup():
    """
    Check for a default configuration when None is passed to logging setup
    """
    config = setup_logging(None)
    assert 'console' in config['handlers']
    assert 'info_file_handler' in config['handlers']
    assert 'error_file_handler' in config['handlers']
    print("Logging setup correctly!")


if __name__ == "__main__":
    test_logging_setup()
