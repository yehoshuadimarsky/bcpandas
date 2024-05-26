"""
Created on Sat Aug  3 23:07:15 2019

@author: ydima
"""

import csv
import logging
import os
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional, Union
from urllib.parse import quote_plus
from re import sub

import pandas as pd
from pandas.io.sql import SQLDatabase, SQLTable
import pyodbc
import sqlalchemy as sa

from bcpandas.constants import (
    IF_EXISTS_OPTIONS,
    IN,
    NEWLINE,
    TABLE,
    BCPandasValueError,
    get_delimiter,
    get_quotechar,
    sql_collation,
)
from bcpandas.utils import bcp, build_format_file, get_temp_file

logger = logging.getLogger(__name__)


class SqlCreds:
    """
    Credential object for all SQL operations. Will automatically also create a SQLAlchemy
    engine that uses `pyodbc` as the DBAPI, and store it in the `self.engine` attribute.

    If `username` and `password` are not provided, `with_krb_auth` will be `True`.

    Only supports SQL based logins, not Active Directory or Azure AD.

    Parameters
    ----------
    server : str
    database : str
    username : str, optional
    password : str, optional
    driver_version : int, optional
        The version of the Microsoft ODBC Driver for SQL Server to use. Defaults to the latest
        version.
    odbc_kwargs : dict of {str, str or int}, optional
        additional keyword arguments, to pass into ODBC connection string,
        such as Encrypted='yes'

    Returns
    -------
    `bcpandas.SqlCreds`
    """

    def __init__(
        self,
        server: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver_version: Optional[int] = None,
        port: int = 1433,
        odbc_kwargs: Optional[Dict[str, Union[str, int]]] = None,
    ):
        self.server = server
        self.database = database
        self.port = port
        self.odbc_kwargs = odbc_kwargs

        if driver_version is None:
            all_drivers: List[str] = pyodbc.drivers()
            driver_candidates: List[str] = [
                d.split("Driver ")[-1].split(" ")[0] for d in all_drivers if "SQL Server" in d
            ]
            new_driver_version: int = max(int(v) for v in driver_candidates if v.isnumeric())
            self.driver = f"{{ODBC Driver {new_driver_version} for SQL Server}}"
            self.driver_version = new_driver_version  # simplifies copy construction
        else:
            self.driver = f"{{ODBC Driver {driver_version} for SQL Server}}"
            self.driver_version = driver_version  # simplifies copy construction

        # Append a comma for use in connection strings (optionally blank)
        if port:
            port_str = f",{self.port}"
        else:
            port_str = ""

        db_url = (
            f"Driver={self.driver};Server=tcp:{self.server}{port_str};Database={self.database};"
        )
        if username and password:
            self.username = username
            self.password = password
            self.with_krb_auth = False
            db_url += f"UID={username};PWD={password};"
        else:
            self.username = ""
            self.password = ""
            self.with_krb_auth = True
            db_url += "Trusted_Connection=yes;"

        self_msg = sub(r"password=\'.*\'", "password=[REDACTED]", str(self))
        logger.info(f"Created creds:\t{self_msg}")

        # construct the engine for sqlalchemy
        if odbc_kwargs:
            db_url += ";".join(f"{k}={v}" for k, v in odbc_kwargs.items())
        conn_string = f"mssql+pyodbc:///?odbc_connect={quote_plus(db_url)}"
        self.engine = sa.engine.create_engine(conn_string)
        engine_msg = sub("PWD%3D.*%3B", "PWD%3D[REDACTED]%3B", str(self.engine))

        logger.info(f"Created engine for sqlalchemy:\t{engine_msg}")

    @classmethod
    def from_engine(cls, engine: sa.engine.base.Engine) -> "SqlCreds":
        """
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
            if "odbc_connect" in engine.url.query:
                conn_url = engine.url.query["odbc_connect"].split(";")
                # convert into dict
                conn_dict = {
                    x.split("=")[0].lower(): x.split("=")[1] for x in conn_url if "=" in x
                }

                if "," in conn_dict["server"]:
                    conn_dict["port"] = int(conn_dict["server"].split(",")[1])
                sql_creds = cls(
                    server=conn_dict["server"].replace("tcp:", "").split(",")[0],
                    database=conn_dict["database"],
                    username=conn_dict.get("uid", None),
                    password=conn_dict.get("pwd", None),
                    port=conn_dict.get("port", None),
                )
            elif "driver" in engine.url.query:
                sql_creds = cls(
                    server=engine.url.host,
                    database=engine.url.database,
                    username=engine.url.username,
                    password=engine.url.password,
                    port=engine.url.port,
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


def _sql_item_exists(sql_type: str, schema: str, table_name: str, creds: SqlCreds) -> bool:
    _qry = dedent(
        """
        SELECT *
        FROM INFORMATION_SCHEMA.{_typ}S
        WHERE TABLE_SCHEMA = '{_schema}'
        AND TABLE_NAME = '{_tbl}'
        """.format(
            _typ=sql_type.upper(), _schema=schema, _tbl=table_name
        )
    )
    res = pd.read_sql_query(sql=_qry, con=creds.engine)
    return res.shape[0] > 0


def _create_table(
    schema: str,
    table_name: str,
    creds: SqlCreds,
    df: pd.DataFrame,
    if_exists: str,
    dtype: Optional[dict] = None,
):
    """use pandas' own code to create the table and schema"""

    with creds.engine.begin() as conn:
        sql_db = SQLDatabase(conn, schema=schema)
        table = SQLTable(
            table_name,
            sql_db,
            frame=df,
            index=False,  # already set as new col earlier if index=True
            if_exists=if_exists,
            index_label=None,
            schema=schema,
            dtype=dtype,
        )
        table.create()


def _handle_cols_for_append(
    df: pd.DataFrame,
    table_name: str,
    creds: SqlCreds,
    sql_item_exists: bool,
    schema: str,
    if_exists: str,
):
    cols_dict = None
    if if_exists == "append":
        # get dict of column names -> order of column
        cols_dict = dict(
            pd.read_sql_query(
                dedent(
                    """
                SELECT COLUMN_NAME, ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{_schema}'
                AND TABLE_NAME = '{_tbl}'
            """.format(
                        _schema=schema, _tbl=table_name
                    )
                ),
                creds.engine,
            ).values
        )

        # check that column names match in db and dataframe exactly
        if sql_item_exists:
            # the db cols are always strings, unlike df cols
            extra_cols = [str(x) for x in df.columns if str(x) not in cols_dict.keys()]
            if extra_cols:
                raise BCPandasValueError(
                    f"Column(s) detected in the dataframe that are not in the database, "
                    f"cannot have new columns if `if_exists=='append'`, "
                    f"the extra column(s): {extra_cols}"
                )
    return cols_dict


def _prepare_table(
    df: pd.DataFrame,
    table_name: str,
    creds: SqlCreds,
    sql_item_exists: bool,
    sql_type: str,
    schema: str,
    if_exists: str,
    dtype: Optional[dict],
) -> None:
    """
    Prepares the destination SQL table, handling the `if_exists` param.
    """
    if if_exists == "fail":
        if sql_item_exists:
            raise BCPandasValueError(
                f"The {sql_type} called {schema}.{table_name} already exists, "
                f"`if_exists` param was set to `fail`."
            )
        else:
            _create_table(
                schema=schema,
                table_name=table_name,
                creds=creds,
                df=df,
                if_exists=if_exists,
                dtype=dtype,
            )
    elif if_exists == "replace":
        _create_table(
            schema=schema,
            table_name=table_name,
            creds=creds,
            df=df,
            if_exists=if_exists,
            dtype=dtype,
        )
    elif if_exists == "append":
        if not sql_item_exists:
            _create_table(
                schema=schema,
                table_name=table_name,
                creds=creds,
                df=df,
                if_exists=if_exists,
                dtype=dtype,
            )


def _validate_args(
    df: pd.DataFrame,
    sql_type: str,
    if_exists: str,
    batch_size: Optional[int],
) -> None:
    assert sql_type == TABLE, "only supporting table, not view, for now"
    assert if_exists in IF_EXISTS_OPTIONS

    if df.columns.has_duplicates:
        raise BCPandasValueError(
            "Columns with duplicate names detected, SQL requires that column names be unique. "
            f"Duplicates: {df.columns[df.columns.duplicated(keep=False)]}"
        )

    if batch_size is not None:
        if batch_size == 0:
            raise BCPandasValueError("Param batch_size can't be 0")
        if batch_size > df.shape[0]:
            raise BCPandasValueError(
                "Param batch_size can't be larger than the number of rows in the DataFrame"
            )


def to_sql(
    df: pd.DataFrame,
    table_name: str,
    creds: SqlCreds,
    sql_type: str = "table",
    schema: str = "dbo",
    index: bool = True,
    if_exists: str = "fail",
    batch_size: Optional[int] = None,
    use_tablock: bool = False,
    debug: bool = False,
    bcp_path: Optional[str] = None,
    dtype: Optional[dict] = None,
    process_dest_table: bool = True,
    print_output: bool = True,
    delimiter: Optional[str] = None,
    quotechar: Optional[str] = None,
    encoding: Optional[str] = None,
    work_directory: Optional[Path] = None,
    collation: str = sql_collation,
    identity_insert: bool = False,
):
    """
    Writes the pandas DataFrame to a SQL table or view.

    Will write all columns to the table or view. If the destination table/view doesn't exist, will create it.
    Assumes the SQL table/view has the same number, name, and type of columns.
    To only write parts of the DataFrame, filter it beforehand and pass that to this function.
    Unlike the pandas counterpart, if the DataFrame has no rows, nothing will happen.

    Parameters
    ----------
    df : pandas.DataFrame
    table_name : str
        Name of SQL table or view, without the schema
    creds : bcpandas.SqlCreds
        The credentials used in the SQL database.
    sql_type : {'table'}, can only be 'table'
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
        * append: Insert new values to the existing table. Matches the dataframe columns to the database columns by name.
            If the database table exists then the dataframe cannot have new columns that aren't in the table,
            but conversely table columns can be missing from the dataframe.

    batch_size : int, optional
        Rows will be written in batches of this size at a time. By default, BCP sets this to 1000.
    use_tablock : bool, default False
        Whether to acquire a table-level lock rather than row-level locks to improve performance.
        Setting this option allows for larger batch sizes.
    debug : bool, default False
        If True, will not delete the temporary CSV and format files, and will output their location.
    bcp_path : str, default None
        The full path to the BCP utility, useful if it is not in the PATH environment variable
    dtype: dict, default None
        A dict with keys the names of columns and values SqlAlchemy types for defining their types. These are
        directly passed into pandas' API
    process_dest_table: bool, default True
        Internal: Only to be used when calling directly from within pandas using the `sql_engine` param.
        If False, then will skip preparing the destination table via the `if_exists` param,
        and will only attempt to insert data.
        You should generally not set this param yourself.
    print_output: bool, default True
        Whether to print output to STDOUT in real time. Regardless, the output will be logged.
        Added in version 1.3
    delimiter: str, default None
        Optional delimiter to use, otherwise will use the result of `constants.get_delimiter`
    quotechar: str, default None
        Optional quotechar to use, otherwise will use the result of `constants.get_quotechar`
    encoding: str, default None
        Optional encoding to use for writing the BCP data-file. Defaults to `utf-8`.
    work_directory: pathlib.Path, default None
        Optional directory where temporary files are written to. If not provided, defaults to the
        system-default for temporary files.
    identity_insert: bool, default False
        Specifies that identity value or values in the imported data file are to be used for the identity column.

    Notes
    -----
    If `delimiter` and/or `quotechar` are specified, you must ensure that those characters
    are not present in the actual data.
    """
    # validation
    if df.shape[0] == 0 or df.shape[1] == 0:
        return

    _validate_args(df=df, sql_type=sql_type, if_exists=if_exists, batch_size=batch_size)

    if index:
        df = df.reset_index()

    delim = get_delimiter(df) if delimiter is None else delimiter
    _quotechar = get_quotechar(df) if quotechar is None else quotechar

    # save to temp path
    csv_file_path = get_temp_file(work_directory)
    # replace bools with 1 or 0, this is what pandas native does when writing to SQL Server
    df.replace({True: 1, False: 0}).to_csv(
        path_or_buf=csv_file_path,
        sep=delim,
        header=False,
        index=False,  # already set as new col earlier if index=True
        quoting=csv.QUOTE_MINIMAL,  # pandas default
        quotechar=_quotechar,
        lineterminator=NEWLINE,
        doublequote=True,
        escapechar=None,  # not needed, as using doublequote
        encoding=encoding,
    )
    logger.debug(f"Saved dataframe to temp CSV file at {csv_file_path}")

    # build format file
    fmt_file_path = get_temp_file(work_directory)

    sql_item_exists = _sql_item_exists(
        sql_type=sql_type, schema=schema, table_name=table_name, creds=creds
    )

    cols_dict = _handle_cols_for_append(
        df=df,
        table_name=table_name,
        creds=creds,
        sql_item_exists=sql_item_exists,
        schema=schema,
        if_exists=if_exists,
    )

    fmt_file_txt = build_format_file(
        df=df, delimiter=delim, db_cols_order=cols_dict, collation=collation
    )
    with open(fmt_file_path, "w") as ff:
        ff.write(fmt_file_txt)
    logger.debug(f"Created BCP format file at {fmt_file_path}")

    try:
        if process_dest_table:
            _prepare_table(
                df=df,
                table_name=table_name,
                creds=creds,
                sql_item_exists=sql_item_exists,
                sql_type=sql_type,
                schema=schema,
                if_exists=if_exists,
                dtype=dtype,
            )

        # BCP the data in
        bcp(
            sql_item=table_name,
            direction=IN,
            flat_file=csv_file_path,
            format_file_path=fmt_file_path,
            creds=creds,
            print_output=print_output,
            sql_type=sql_type,
            schema=schema,
            batch_size=batch_size,
            use_tablock=use_tablock,
            bcp_path=bcp_path,
            identity_insert=identity_insert,
        )
    finally:
        if not debug:
            logger.debug("Deleting temp CSV and format files")
            os.remove(csv_file_path)
            os.remove(fmt_file_path)
        else:
            logger.debug(
                f"`to_sql` DEBUG mode, not deleting the files. CSV file is at "
                f"{csv_file_path}, format file is at {fmt_file_path}"
            )
