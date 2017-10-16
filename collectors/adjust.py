import logging
import os

import click
import pandas as pd

from collectors.common import load, connect, write_to_file, load_settings

ADJUST_URL_BASE = "https://api.adjust.com/kpis/v1/{app_key}"
logger = logging.getLogger(__name__)


@click.command('adjust')
@click.argument('config_file', type=click.Path(exists=True, readable=True))
def adjust_cmd(config_file) -> None:
    """
    Collect data from adjust.com about DAUS/WAUS/MAUS and Installs

    Example config
    ---
    load_path: /tmp/adjust_daus
    vertica:
      dsn: vertica
      table: adjust_daily_active_users
    adjust:
      token: abcdefg
      apps:
        firefox: abc123
        focus: abc123

    :param dsn: odbc dsn to use for vertica connection, defaults to "vertica"
    :param config_file: configuration file containing credentials and keys for adjust.com
    """

    # Setup
    settings = load_settings(config_file)
    adjust_settings = settings['adjust']
    vertica_settings = settings['vertica']
    load_path = settings['load_path']

    # Grab a vertica connection
    cursor = connect(vertica_settings['dsn'])

    # Collect the data from adjust.com
    output_file = collect(adjust_settings, load_path)

    reject_file = os.path.join(load_path, "rejects")
    exception_file = os.path.join(load_path, "exceptions")
    load(cursor, vertica_settings['table'], output_file, reject_file, exception_file)


def collect(adj_settings, load_path) -> str:
    """
    Collect the data from adjust
    :param adj_settings: dictionary of settings specific to adjust.com
    """
    apps = adj_settings['apps']
    token = adj_settings['token']

    df = merge_apps(apps, token)
    return write_to_file(df, load_path)


def merge_apps(apps, adjust_token) -> pd.DataFrame:
    """
    Call collect_app on each of the apps that we are tracking in adjust.com
    :param apps: dict of app names and ids
    :param adjust_token: access token for adjust.com api
    :return:
    """
    frames = []
    for app, key in iter(apps):
        url = build_dau_url(key, adjust_token)
        df = collect_app(app, url)
        frames.append(df)

    df = pd.concat(frames)

    return df


def build_dau_url(app_key, token) -> str:
    """
    Build the adjust.com URL for daily activity
    """
    url = ADJUST_URL_BASE.format(app_key=app_key)
    url += ".csv"
    url += "?user_token={user_token}".format(user_token=token)
    url += "&kpis=daus,waus,maus,installs"
    url += "&start_date=2000-01-01"
    url += "&end_date=2030-01-01"
    url += "&grouping=day,os_names"
    url += "&os_names=android,ios"
    return url


def collect_app(app_name, url) -> pd.DataFrame:
    """
    Load the activity CSV for a given app
    :param app_name: Name of the app that this data is for
    :param url: URL where the data can be retrieved
    :return:
    """
    # Get the csv file from adjust and load it into pandas
    col_names = ['adj_date', 'os', 'daus', 'waus', 'maus', 'installs']
    df = pd.read_csv(url, sep=",", header=0, names=col_names)

    # Force convert missing to 0 for installs, this gets us through an unnecessary type conversion
    df['installs'] = df['installs'].fillna(0).astype(int)

    # Append the app name
    df['app'] = app_name
    logging.info("{} collected".format(app_name))

    return df


if __name__ == '__main__':
    adjust_cmd()
