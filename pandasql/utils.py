# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:07:15 2019

@author: ydima
"""

import logging
import os
import random
import string
import subprocess
import tempfile
from io import StringIO

import pandas as pd

from .constants import (
    DELIMITER,
    DIRECTIONS,
    IN,
    NEWLINE,
    OUT,
    QUERY,
    QUERYOUT,
    SQLCHAR,
    TABLE,
    sql_collation,
)

# TODO add logging
logger = logging.getLogger(__name__)


def bcp(
    sql_item: str,
    direction: str,
    flat_file: str,
    creds,
    sql_type: str = "table",
    schema: str = "dbo",
    format_file: str = None,
    batch_size: int = 100000,
):

    combos = {TABLE: [IN, OUT], QUERY: [QUERYOUT]}
    direc = direction.lower()
    # validation
    if direc not in DIRECTIONS:
        raise ValueError(
            f"Param 'direction' must be one of {DIRECTIONS}, you passed {direc}"
        )
    if direc not in combos[sql_type]:
        raise ValueError(
            f"Wrong combo of direction and SQL object, you passed {sql_type} and {direc} ."
        )

    # auth
    if creds.with_krb_auth:
        auth = ["-T"]
    else:
        auth = ["-U", creds.username, "-P", creds.password]

    # prepare SQL item string
    if sql_type == QUERY:
        # remove newlines for queries, otherwise messes up BCP
        sql_item_string = "".join(sql_item.splitlines()) 
    else:
        sql_item_string = f"{schema}.{sql_item}"

    # construct BCP command
    bcp_command = [
        "bcp",
        sql_item_string,
        direc,
        flat_file,
        "-S",
        creds.server,
        "-d",
        creds.database,
        "-b",
        str(batch_size),
    ] + auth

    # formats
    if direc == IN:
        bcp_command += ["-f", format_file]
    elif direc in (OUT, QUERYOUT):
        bcp_command += [
            "-c",  # marking as character data, not Unicode (maybe make as param?)
            "-t,",  # marking the delimiter as a comma (maybe also make as param?)
        ]

    # execute
    # TODO better logging and error handling the return stream
    result = subprocess.run(bcp_command, stderr=subprocess.PIPE)
    if result.returncode:
        raise Exception(f"Bcp command failed. Details:\n{result}")


def get_temp_file():
    """
    Returns full path to a temporary file without creating it.
    """
    tmp_dir = tempfile.gettempdir()
    file_path = os.path.join(
        tmp_dir, "".join(random.choices(string.ascii_letters + string.digits, k=21))
    )
    return file_path


def _escape(input_string):
    return (
        input_string.replace('"', '\\"')
        .replace("'", "\\'")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


def build_format_file(df):
    """
    Creates the non-xml SQL format file. Puts 2 spaces between each section.
    See https://docs.microsoft.com/en-us/sql/relational-databases/import-export/non-xml-format-files-sql-server
    for the specification of the file.

    # TODO add params/options to control the char type (not just SQLCHAR),
        and the ability to skip destination columns

    Parameters
    -------------
    df : pandas DataFrame

    Returns
    -------------
    A string containing the format file
    """
    _space = "   " 
    format_file = f"9.0\n{len(df.columns)}\n"
    for col_num, col_name in enumerate(df.columns, start=1):
        _delim = (
            DELIMITER if col_num != len(df.columns) else NEWLINE
        )  # last col gets a newline sep
        _line = _space.join([
           {col_num},   # flat file column number
           {SQLCHAR}, 
           str(0),  # char prefix 
           str(0), 
           {_escape(_delim)},  # separator 
           {col_num},  # sql column number 
           {col_name},  # sql column name, sort of optional 
           {sql_collation}, 
           "\n", 
         ]) 
        format_file += _line
    return format_file


def _get_sql_create_statement(df, table_name, schema="dbo"):
    """
    Creates a SQL drop and re-create statement corresponding to the columns list of the object.
    
    Parameters
    -------------
    df : pandas DataFrame
    table_name : str
        name of the new table
    
    Returns
    -------------
    SQL code to create the table
    """
    sql_cols = ",".join(map(lambda x: f"[{x}] nvarchar(max)", df.columns))
    sql_command = (
        f"if object_id('[dbo].[{table_name}]', 'U') "
        f"is not null drop table [dbo].[{table_name}];"
        f"create table [dbo].[{table_name}] ({sql_cols});"
    )
    return sql_command


def sqlcmd(server, database, command, username=None, password=None):
    """
    Runs the input command against the database and returns the output if it is a table.
    Leave username and password to None if you intend to use Kerberos integrated authentication.
    
    Parameters
    -------------------    
        server : str
            SQL Server
        database : str
            Name of the default database for the script
        command : str
            SQL command to be executed against the server
        username : str
            Username to use for login
        password : str
            Password to use for login

    Returns
    --------------------
    Pandas.DataFrame 
        Returns a table if the command has an output. Returns None if the output does not return anything.
    """
    if not username or not password:
        auth = ["-E"]
    else:
        auth = ["-U", username, "-P", password]
    command = "set nocount on;" + command
    sqlcmd_command = (
        ["sqlcmd", "-S", server, "-d", database, "-b"]
        + auth
        + ["-s,", "-W", "-Q", command]
    )
    result = subprocess.run(
        sqlcmd_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if result.returncode:
        print(result.stdout)
        raise Exception(f"Sqlcmd command failed. Details:\n{result}")
    output = StringIO(result.stdout.decode("ascii"))
    first_line_output = output.readline().strip()
    if first_line_output == "":
        header = None
    else:
        header = "infer"
    output.seek(0)
    try:
        result = pd.read_csv(filepath_or_buffer=output, skiprows=[1], header=header)
    except pd.errors.EmptyDataError:
        result = None
    return result
