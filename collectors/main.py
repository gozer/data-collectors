import json
import logging
import logging.config
import os

import click
import pkg_resources
import yaml

logger = logging.getLogger(__name__)


@click.group()
@click.option("--log-conf",
              type=click.Path(exists=True, readable=True),
              help="Path to YAML logging configuration",
              default=None)
def cli(log_conf):
    """
    Main entry point for the collectors
    """
    setup_logging(log_conf)


def setup_logging(logging_config=None):
    """
    Setup the logging configuration

    :param logging_config: path pointing to the logging configuration YAML File
    :return: configuration dictionary
    :rtype: dict
    """
    if logging_config is not None:
        _, extension = os.path.splitext(logging_config)
        if extension == '.yml' or extension == ".yaml":
            with open(logging_config, "r") as f:
                config = yaml.safe_load(f)
        elif extension == '.json':
            with open(logging_config, 'r') as f:
                config = json.load(f)
        else:
            raise Exception("%s is not a supported extension" % extension)
    else:
        pkg = __name__
        path = '/'.join(('defaults', 'default_log_config.yml'))
        with pkg_resources.resource_stream(pkg, path) as f:
            config = yaml.load(f)

    logging.config.dictConfig(config)
    logger.info("Logging configured")
    return config


def load_settings(yaml_file):
    """
    Load settings for the collectors
    :param yaml_file: YAML file containing job configuration
    :return: dict of settings
    """
    with open(yaml_file, 'r') as f:
        return yaml.safe_load(f.read())


if __name__ == "__main__":
    cli()
