import json
import logging
import logging.config
import os

import click
import pkg_resources
import yaml

logger = logging.getLogger(__name__)


def load_yaml(ctx, param, yaml_file) -> dict:
    """
    Load settings for the collectors
    :param yaml_file: YAML file containing job configuration
    :return: dict of settings
    """
    with open(yaml_file, 'r') as f:
        return yaml.safe_load(f.read())


def setup_logging(logging_config=None) -> dict:
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


@click.group()
@click.option("--log-conf", type=click.Path(exists=True, readable=True),
              help="Path to YAML logging configuration", default=None)
@click.option("--config", callback=load_yaml, default="/etc/data-collectors/data-collectors.yml",
              type=click.Path(exists=True, readable=True), help='Base configuration for all jobs')
@click.pass_context
def cli(ctx, log_conf, config) -> None:
    """
    Main entry point for the collectors
    """
    setup_logging(log_conf)
    ctx.obj['config'] = config


if __name__ == "__main__":
    from collectors.adjust import adjust_cmd
    from collectors.redash import redash_cmd

    cli.add_command(adjust_cmd)
    cli.add_command(redash_cmd)
    cli(obj={})
