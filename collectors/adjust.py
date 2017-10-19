import logging
import os

import click
import pandas as pd

from collectors.common import load, connect, write_to_file

ADJUST_URL_BASE = "https://api.adjust.com/kpis/v1/{app_key}"
logger = logging.getLogger(__name__)


def collect(adj_settings, load_path, job) -> str:
    """
    Collect the data from adjust
    :param adj_settings: dictionary of settings specific to adjust.com
    """
    apps = adj_settings['apps']
    token = adj_settings['token']

    df = merge_apps(apps, token, job)
    return write_to_file(df, load_path)


def merge_apps(apps, adjust_token, jobClass) -> pd.DataFrame:
    """
    Call collect_app on each of the apps that we are tracking in adjust.com
    :param apps: dict of app names and ids
    :param adjust_token: access token for adjust.com api
    :return:
    """
    frames = []
    for app, key in apps.items():
        job = jobClass()
        url = job.build_url(key, adjust_token)
        df = job.collect(app, url)
        frames.append(df)

    df = pd.concat(frames)

    return df


class DailyActiveUsers(object):
    NAME = 'daily_active_users'

    def build_url(self, app_key, token) -> str:
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

    def collect(self, app_name, url) -> pd.DataFrame:
        """
        Load the activity CSV for a given app
        :param app_name: Name of the app that this data is for
        :param url: URL where the data can be retrieved
        :return: DataFrame of results
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


class Retention(object):
    NAME = 'retention'

    def build_url(self, app_key, token) -> str:
        url = ADJUST_URL_BASE.format(app_key=app_key)
        url += "/cohorts.csv"
        url += "?user_token={user_token}".format(user_token=token)
        url += "&kpis=retention_rate"
        url += "&start_date=2000-01-01"
        url += "&end_date=2030-01-01"
        url += "&grouping=day,os_names"
        url += "&os_names=android,ios"
        return url

    def collect(self, app_name, url) -> pd.DataFrame:
        """
        Load retention data for a given app
        :param app_name: Name of the app
        :param url: Constructed URL for requesting data from adjust.com
        :return: DataFrame of results
        """
        col_names = ['adj_date', 'os', 'period', 'retention_rate']
        df = pd.read_csv(url, sep=",", header=0, names=col_names)
        df['app'] = app_name
        logging.info("{} collected".format(app_name))
        return df


def select_job(ctx, param, value) -> object:
    """
    Select the appropriate job
    """
    JOBS = dict(retention=Retention, daily_active_users=DailyActiveUsers)
    try:
        return JOBS[value]
    except:
        raise click.BadParameter("Job must be one of {}".format(JOBS.keys()))


@click.command('adjust')
@click.option("--table", type=str, required=True, help="Name of the target table where the load should be persisted")
@click.option("--job", callback=select_job, required=True)
@click.pass_context
def adjust_cmd(ctx, table, job) -> None:
    # Grab a vertica connection
    cursor = connect(ctx.obj['config']['vertica']['dsn'])

    # Setup the load_path
    load_path = os.path.join(ctx.obj['config']['data_dir'], job.NAME)

    # Collect the data from adjust.com
    output_file = collect(ctx.obj['config']['adjust'], load_path, job)

    reject_file = os.path.join(load_path, "rejects")
    exception_file = os.path.join(load_path, "exceptions")
    load(cursor, table, output_file, reject_file, exception_file)


if __name__ == '__main__':
    adjust_cmd()
