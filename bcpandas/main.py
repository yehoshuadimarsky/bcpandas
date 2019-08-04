# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:07:15 2019

@author: ydima
"""

import csv
import logging
import os

import pandas as pd

from .constants import (
    DELIMITER,
    IF_EXISTS_OPTIONS,
    IN,
    NEWLINE,
    OUT,
    QUOTECHAR,
    SQL_TYPES,
    TABLE,
    VIEW,
)
from .utils import (
    _get_sql_create_statement,
    bcp,
    build_format_file,
    get_temp_file,
    sqlcmd,
)

# TODO add logging
logger = logging.getLogger(__name__)


class SqlCreds:
    """
    Credential object for all SQL operations
    """

    def __init__(self, server, database, username=None, password=None):
        if not server or not database:
            raise ValueError(
                f"Server and database can't be None, you passed {server}, {database}"
            )
        self.server = server
        self.database = database
        if username and password:
            self.username = username
            self.password = password
            self.with_krb_auth = False
        else:
            self.with_krb_auth = True

    def __repr__(self):
        # adopted from https://github.com/erdewit/ib_insync/blob/master/ib_insync/objects.py#L51
        clsName = self.__class__.__qualname__
        kwargs = ", ".join(
            f"{k}={v!r}" for k, v in self.__dict__.items() if k != "password"
        )
        if hasattr(self, "password"):
            kwargs += ", password=[REDACTED]"
        return f"{clsName}({kwargs})"

    __str__ = __repr__


# DataFrame
def to_sql(
    df: pd.DataFrame,
    table_name: str,
    creds: SqlCreds,
    sql_type: str = "table",
    schema: str = "dbo",
    index: bool = False,
    if_exists: str = "replace",
    batch_size: int = 10000,
    debug: bool = False,
):
    # validation
    assert sql_type in SQL_TYPES
    assert if_exists in IF_EXISTS_OPTIONS

    # save to temp path
    csv_file_path = get_temp_file()
    df.to_csv(
        path_or_buf=csv_file_path,
        sep=DELIMITER,
        header=False,
        index=False,
        quoting=csv.QUOTE_MINIMAL,  # pandas default
        quotechar=QUOTECHAR,
        line_terminator=NEWLINE,
        doublequote=True,
        escapechar=None,  # not needed, as using doublequote
    )

    # build format file
    fmt_file_path = get_temp_file()
    fmt_file_txt = build_format_file(df=df)
    with open(fmt_file_path, "w") as ff:
        ff.write(fmt_file_txt)

    try:
        if if_exists == "fail":
            # TODO fix
            raise NotImplementedError()
        elif if_exists == "replace":
            sqlcmd(
                command=_get_sql_create_statement(
                    df=df, table_name=table_name, schema=schema
                ),  # TODO fix
                server=creds.server,
                database=creds.database,
                username=creds.username,
                password=creds.password,
            )

        # either way, BCP data in
        bcp(
            sql_item=table_name,
            direction=IN,
            flat_file=csv_file_path,
            format_file_path=fmt_file_path,
            creds=creds,
            sql_type=sql_type,
            schema=schema,
            batch_size=batch_size,
        )
    finally:
        if not debug:
            os.remove(csv_file_path)
            os.remove(fmt_file_path)


def read_sql(
    table_name: str,
    creds: SqlCreds,
    sql_type: str = "table",
    schema: str = "dbo",
    mssql_odbc_driver_version: int = 17,
    batch_size: int = 10000,
):
    # check params
    assert sql_type in SQL_TYPES
    assert mssql_odbc_driver_version in {
        13,
        17,
    }, "SQL Server ODBC Driver must be either 13 or 17"

    # ensure pyodbc installed
    try:
        import pyodbc
    except ImportError:
        raise ImportError("pyodbc library required.")

    # set up objects
    if ";" in table_name:
        raise ValueError(
            "The SQL item cannot contain the ';' character, it interferes with getting the column names"
        )

    # TODO not sure how to support Kerberos here
    db_conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver {mssql_odbc_driver_version} for SQL Server}};SERVER={creds.server};"
        f"DATABASE={creds.database};UID={creds.username};PWD={creds.password}"
    )

    # read top 2 rows of query to get the columns
    _from_clause = table_name if sql_type in (TABLE, VIEW) else f"({table_name})"

    cols = pd.read_sql_query(
        sql=f"SELECT TOP 2 * FROM {_from_clause} as qry", con=db_conn
    ).columns
    file_path = get_temp_file()
    try:
        bcp(
            sql_item=table_name,
            direction=OUT,
            flat_file=file_path,
            creds=creds,
            sql_type=sql_type,
            schema=schema,
            batch_size=batch_size,
        )
        return pd.read_csv(
            filepath_or_buffer=file_path, header=None, names=cols, index_col=False
        )
    finally:
        os.remove(file_path)
