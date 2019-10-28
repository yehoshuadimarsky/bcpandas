# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:07:15 2019

@author: ydima
"""

import csv
import logging
import os
import urllib

import pandas as pd
import pyodbc
import sqlalchemy as sa

from .constants import (
    BCPandasValueError,
    get_delimiter,
    _DELIMITER_OPTIONS,
    IF_EXISTS_OPTIONS,
    IN,
    NEWLINE,
    OUT,
    QUERYOUT,
    QUOTECHAR,
    SQL_TYPES,
    TABLE,
    VIEW,
    QUERY,
)
from .utils import bcp, build_format_file, get_temp_file, sqlcmd

logger = logging.getLogger(__name__)


class SqlCreds:
    """
    Credential object for all SQL operations. Will automatically also create a SQLAlchemy 
    engine that uses `pyodbc` as the DBAPI, and store it in the `self.engine` attribute.

    If `username` and `password` are not provided, `with_krb_auth` will be `True`.

    Parameters
    ----------
    server : str
    database : str
    username : str, optional
    password : str, optional
    driver_version : int, default 17
        The version of the Microsoft ODBC Driver for SQL Server to use 
    **kwargs : additional keyword arguments, to pass into ODBC connection string, 
        such as Encrypted='yes'
    
    Returns
    -------
    `bcpandas.SqlCreds`
    """

    def __init__(self, server, database, username=None, password=None, driver_version=17, **kwargs):
        if not server or not database:
            raise BCPandasValueError(
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
        logger.info(f"Created creds:\t{self}")

        # construct the engine for sqlalchemy
        driver = f"{{ODBC Driver {driver_version} for SQL Server}}"
        db_url = (
            f"Driver={driver};Server=tcp:{self.server},1433;Database={self.database};"
            f"UID={self.username};PWD={self.password}"
        )
        if kwargs:
            db_url += ";".join(f"{k}={v}" for k, v in kwargs.items())
        conn_string = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(db_url)}"
        self.engine = sa.engine.create_engine(conn_string)

        logger.info(f"Created engine for sqlalchemy:\t{self.engine}")

    @classmethod
    def from_engine(cls, engine):
        """
        Alternate constructor from a `sqlalchemy.engine.base.Engine` object.

        Alternate constructor, from a `sqlalchemy.engine.base.Engine` that uses `pyodbc` as the DBAPI 
        (which is the SQLAlchemy default for MS SQL) and using an exact PyODBC connection string (not DSN or hostname).
        See https://docs.sqlalchemy.org/en/13/dialects/mssql.html#connecting-to-pyodbc for more.
        
        Parameters
        ----------
        engine : `sqlalchemy.engine.base.Engine`
            The SQLAlchemy engine object, configured as described above

        Returns
        -------
        `bcpandas.SqlCreds`
        """
        try:
            # get the odbc url part from the engine, split by ';' delimiter
            conn_url = engine.url.query["odbc_connect"].split(";")
            # convert into dict
            conn_dict = {x.split("=")[0]: x.split("=")[1] for x in conn_url if "=" in x}

            sql_creds = cls(
                server=conn_dict["Server"].replace("tcp:", "").replace(",1433", ""),
                database=conn_dict["Database"],
                username=conn_dict["UID"],
                password=conn_dict["PWD"],
            )
            # add Engine object as attribute
            sql_creds.engine = engine
            return sql_creds
        except (KeyError, AttributeError) as ex:
            raise BCPandasValueError(
                f"The supplied 'engine' object could not be parsed correctly, try creating a SqlCreds object manually."
                f"\nOriginal Error: \n {ex}"
            )

    def __repr__(self):
        # adopted from https://github.com/erdewit/ib_insync/blob/master/ib_insync/objects.py#L51
        clsName = self.__class__.__qualname__
        kwargs = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items() if k != "password")
        if hasattr(self, "password"):
            kwargs += ", password=[REDACTED]"
        return f"{clsName}({kwargs})"

    __str__ = __repr__


def to_sql(
    df,
    table_name,
    creds,
    sql_type="table",
    schema="dbo",
    index=True,
    if_exists="fail",
    batch_size=None,
    debug=False,
):
    """
    Writes the pandas DataFrame to a SQL table or view.

    Will write all columns to the table or view. 
    Assumes the SQL table or view has the same number, name, and type of columns.
    To only write parts of the DataFrame, filter it beforehand and pass that to this function.

    Parameters
    ----------
    df : pandas.DataFrame
    table_name : str
        Name of SQL table or view, without the schema
    creds : bcpandas.SqlCreds
        The credentials used in the SQL database.
    sql_type : {'table', 'view'}, default 'table'
        The type of SQL object of the destination.
    schema : str, default 'dbo'
        The SQL schema.
    index : bool, default True
        Write DataFrame index as a column. Uses the index name as the column
        name in the table.
    if_exists : {'fail', 'replace', 'append'}, default 'fail'
        How to behave if the table already exists.
        * fail: Raise a BCPandasValueError.
        * replace: Drop the table before inserting new values.
        * append: Insert new values to the existing table.
    batch_size : int, optional
        Rows will be written in batches of this size at a time. By default,
        all rows will be written at once.
    debug : bool, default False
        If True, will not delete the temporary CSV and format files, and will output their location.
    """
    # validation
    assert sql_type in SQL_TYPES
    assert if_exists in IF_EXISTS_OPTIONS

    delim = get_delimiter(df)

    # save to temp path
    csv_file_path = get_temp_file()
    df.to_csv(
        path_or_buf=csv_file_path,
        sep=delim,
        header=False,
        index=False,
        quoting=csv.QUOTE_MINIMAL,  # pandas default
        quotechar=QUOTECHAR,
        line_terminator=NEWLINE,
        doublequote=True,
        escapechar=None,  # not needed, as using doublequote
    )
    logger.debug(f"Saved dataframe to temp CSV file at {csv_file_path}")

    # build format file
    fmt_file_path = get_temp_file()
    fmt_file_txt = build_format_file(df=df, delimiter=delim)
    with open(fmt_file_path, "w") as ff:
        ff.write(fmt_file_txt)
    logger.debug(f"Created BCP format file at {fmt_file_path}")

    try:
        if if_exists == "fail":
            _qry = """SELECT * 
                 FROM INFORMATION_SCHEMA.{_typ}S 
                 WHERE TABLE_SCHEMA = '{_schema}' 
                 AND TABLE_NAME = '{_tbl}'"""
            res = sqlcmd(
                creds=creds,
                command=_qry.format(_typ=sql_type.upper(), _schema=schema, _tbl=table_name),
            )
            if res is not None:
                raise BCPandasValueError(
                    f"The {sql_type} called {schema}.{table_name} already exists, "
                    f"`if_exists` param was set to `fail`."
                )
        elif if_exists == "replace":
            # use pandas' own code to create the table and schema
            from pandas.io.sql import SQLDatabase, SQLTable

            sql_db = SQLDatabase(engine=creds.engine, schema=schema)
            table = SQLTable(
                table_name,
                sql_db,
                frame=df,
                index=index,
                if_exists=if_exists,
                index_label=None,
                schema=schema,
                dtype=None,
            )
            table.create()

        elif if_exists == "append":
            pass  # don't need to do anything

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
            logger.debug(f"Deleting temp CSV and format files")
            os.remove(csv_file_path)
            os.remove(fmt_file_path)
        else:
            logger.debug(
                f"`to_sql` DEBUG mode, not deleting the files. CSV file is at "
                f"{csv_file_path}, format file is at {fmt_file_path}"
            )


def read_sql(table_name, creds, sql_type="table", schema="dbo", batch_size=None, debug=False):
    """
    Reads a SQL table, view, or query into a pandas DataFrame.

    Parameters
    ----------
    table_name : str
        Name of SQL table or view, without the schema, or a query string
    creds : bcpandas.SqlCreds
        The credentials used in the SQL database.
    sql_type : {'table', 'view', 'query'}, default 'table'
        The type of SQL object that the parameter `table_name` is.
    schema : str, default 'dbo'
        The SQL schema of the table or view. If a query, will be ignored.
    batch_size : int, optional
        Rows will be read in batches of this size at a time. By default,
        all rows will be read at once.
    debug : bool, default False
        If True, will not delete the temporary CSV file, and will output its location.

    Returns
    -------
    `pandas.DataFrame`

    Notes
    -----
    Will actually read the SQL table/view/query twice - first using the sqlcmd utility 
    to get the names of the columns (will only read first few rows), then all the rows 
    using BCP.
    """
    # check params
    assert sql_type in SQL_TYPES

    # set up objects
    if ";" in table_name:
        raise BCPandasValueError(
            "The SQL item cannot contain the ';' character, it interferes with getting the column names"
        )

    # read top 2 rows of query to get the columns
    logger.debug("Starting to read first 2 rows using sqlcmd, to get the column names")
    _from_clause = table_name if sql_type in (TABLE, VIEW) else f"({table_name})"
    _existing_data = sqlcmd(creds=creds, command=f"SELECT TOP 2 * FROM {_from_clause} as qry")
    if _existing_data is not None:
        cols = _existing_data.columns
        logger.debug("Successfully read the column names using sqlcmd")
    else:
        raise BCPandasValueError(
            f"No data returned from the SQL item named {table_name} with type of {sql_type}"
        )

    file_path = get_temp_file()
    try:
        bcp(
            sql_item=table_name,
            direction=QUERYOUT if sql_type == QUERY else OUT,
            flat_file=file_path,
            creds=creds,
            sql_type=sql_type,
            schema=schema,
            batch_size=batch_size,
        )
        logger.debug(f"Saved dataframe to temp CSV file at {file_path}")
        return pd.read_csv(
            filepath_or_buffer=file_path,
            sep=_DELIMITER_OPTIONS[0],
            header=None,
            names=cols,
            index_col=False,
        )
    finally:
        if not debug:
            logger.debug(f"Deleting temp CSV file")
            os.remove(file_path)
        else:
            logger.debug(
                f"`read_sql` DEBUG mode, not deleting the file. CSV file is at {file_path}."
            )
