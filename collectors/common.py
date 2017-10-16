import logging
import os
import pyodbc

import yaml

logger = logging.getLogger(__name__)


def load(cursor, table, data_file, reject_file, exception_file) -> None:
    """
    Truncate a vertica table and then execute a load.  This assumes that the source and target schemas
    implicitly match
    :param cursor: pyodbc cursor
    :param table: name of the table to load
    :param data_file: path to load from
    :param reject_file: path to store rejected data
    :param exception_file: path to store encountered exceptions
    :return:
    """
    # Truncate the table before the load since every load is a FULL rewrite
    trunc_tmpl = "TRUNCATE TABLE {tbl}"
    cursor.execute(trunc_tmpl.format(tbl=table))

    files = {
        "data_file": data_file,
        "reject_file": reject_file,
        "exception_file": exception_file
    }

    # Copy the local file
    copy_tmpl = "COPY {table} FROM LOCAL '{data_file}' " \
                "DELIMITER ',' SKIP 1 " \
                "REJECTED DATA '{reject_file}' " \
                "EXCEPTIONS '{exception_file}' " \
                "ABORT ON ERROR DIRECT"
    copy_stmt = copy_tmpl.format(table=table, **files)
    print(copy_stmt)
    cursor.execute(copy_stmt)
    logger.info("Completed loading {table} - #{count} Records".format(
        table=table,
        count=cursor.rowcount
    ))

    commit_sql = "INSERT INTO last_updated (name, updated_at, updated_by) " \
                 "VALUES ('{table}', now(), 'Data-Collectors');".format(table=table)
    cursor.execute(commit_sql)
    cursor.execute("COMMIT;")


def connect(dsn) -> type:
    """
    Obtain an ODBC cursor for vertica
    """
    cnxn = pyodbc.connect("DSN=%s" % dsn)
    logger.info("Database connection established")
    return cnxn.cursor()


def write_to_file(df, load_path, filename="output.csv") -> str:
    """
    Write a dataframe to local storage
    :param df: data frame containing the activity record
    :param load_path: local path to write the data to
    :param filename: name of the file to save the output to
    :return: fully resolved path to the output file
    """
    data_file = os.path.join(load_path, filename)

    if not os.path.exists(load_path):
        os.makedirs(load_path)

    df.to_csv(data_file, index=False)

    return data_file


def load_settings(yaml_file):
    """
    Load settings for the collectors
    :param yaml_file: YAML file containing job configuration
    :return: dict of settings
    """
    with open(yaml_file, 'r') as f:
        return yaml.safe_load(f.read())
