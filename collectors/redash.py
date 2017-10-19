import logging
import os

import click
import pandas as pd

from collectors.common import connect, load, write_to_file

REDASH_URL_TMPL = "https://sql.telemetry.mozilla.org/api/queries/{query_id}/results.csv?api_key={api_key}"

logger = logging.getLogger(__name__)


@click.command('redash')
@click.option("--table", type=str, required=True, help="Name of the target table where the load should be persisted")
@click.option("--api-key", type=str, required=True, help="API Key associated with the redash query")
@click.option("--query_id", type=str, required=True, help="Query ID")
@click.pass_context
def redash_cmd(ctx, table, api_key, query_id) -> None:
    # Setup the load path
    data_dir = ctx.obj['config']['data_dir']
    load_path = os.path.join(data_dir, "redash", query_id)

    # Grab a vertica connection
    cursor = connect(ctx.obj['config']['vertica']['dsn'])
    reject_file = os.path.join(load_path, "rejects")
    exception_file = os.path.join(load_path, "exceptions")

    query_url = REDASH_URL_TMPL.format(query_id=query_id, api_key=api_key)

    output_path = collect(query_url, load_path)
    load(cursor, table, output_path, reject_file, exception_file)


def collect(url, load_path) -> str:
    df = pd.read_csv(url, sep=',')
    return write_to_file(df, load_path)


if __name__ == '__main__':
    redash_cmd()
