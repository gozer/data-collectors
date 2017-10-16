import logging
import os

import click
import pandas as pd

from collectors.common import load_settings, connect, load, write_to_file

REDASH_URL_TMPL = "https://sql.telemetry.mozilla.org/api/queries/{query_id}/results.csv?api_key={api_key}"

logger = logging.getLogger(__name__)


@click.command('redash')
@click.argument('config_file', type=click.Path(exists=True, readable=True))
def redash_cmd(config_file):
    settings = load_settings(config_file)
    redash_settings = settings['redash']
    vertica_settings = settings['vertica']
    load_path = settings['load_path']

    # Grab a vertica connection
    cursor = connect(vertica_settings['dsn'])
    reject_file = os.path.join(load_path, "rejects")
    exception_file = os.path.join(load_path, "exceptions")

    query_url = REDASH_URL_TMPL.format(**redash_settings)

    output_path = collect(query_url, load_path)
    load(cursor, vertica_settings['table'], output_path, reject_file, exception_file)


def collect(url, load_path):
    df = pd.read_csv(url, sep=',')
    return write_to_file(df, load_path)


if __name__ == '__main__':
    redash_cmd()
