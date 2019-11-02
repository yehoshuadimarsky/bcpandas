# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:07:15 2019

@author: ydima
"""

import logging
import os
import random
import string
from subprocess import Popen, PIPE
import tempfile

import pandas as pd

from .constants import (
    BCPandasException,
    BCPandasValueError,
    _DELIMITER_OPTIONS,
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

logger = logging.getLogger(__name__)


def bcp(
    sql_item,
    direction,
    flat_file,
    creds,
    sql_type="table",
    schema="dbo",
    format_file_path=None,
    batch_size=None,
):

    combos = {TABLE: [IN, OUT], QUERY: [QUERYOUT]}
    direc = direction.lower()
    # validation
    if direc not in DIRECTIONS:
        raise BCPandasValueError(
            f"Param 'direction' must be one of {DIRECTIONS}, you passed {direc}"
        )
    if direc not in combos[sql_type]:
        raise BCPandasValueError(
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
        "-q",  # Executes the SET QUOTED_IDENTIFIERS ON statement, needed for Azure SQL DW
    ] + auth

    if batch_size:
        bcp_command += ["-b", str(batch_size)]

    # formats
    if direc == IN:
        bcp_command += ["-f", format_file_path]
    elif direc in (OUT, QUERYOUT):
        bcp_command += [
            "-c",  # marking as character data, not Unicode (maybe make as param?)
            f"-t{_DELIMITER_OPTIONS[0]}",  # marking the delimiter as a comma (maybe also make as param?)
        ]

    # execute
    bcp_command_log = [c if c != creds.password else "[REDACTED]" for c in bcp_command]
    logger.info(f"Executing BCP command now... \nBCP command is: {bcp_command_log}")
    ret_code = run_cmd(bcp_command)
    if ret_code:
        raise BCPandasException(f"Bcp command failed with exit code {ret_code}")


def sqlcmd(creds, command):
    """
    Runs the input command against the database and returns the output if it is a table.
    
    Parameters
    ----------
    creds : bcpandas.SqlCreds
        Creds for the database
    command : str
        SQL command to be executed against the server
    
    Returns
    --------------------
    Pandas.DataFrame or None
        Returns a table if the command has an output. Returns None if the output does not return anything.
    """
    if creds.with_krb_auth:
        auth = ["-E"]
    else:
        auth = ["-U", creds.username, "-P", creds.password]
    if '"' in command:
        raise BCPandasValueError(
            'Cannot have double quotes charachter (") in the command, '
            "raises problems when combining the sqlcmd utility with Python"
        )
    command = f"set nocount on; {command} "
    sqlcmd_command = (
        ["sqlcmd", "-S", creds.server, "-d", creds.database, "-b"]
        + auth
        # set quoted identifiers ON, needed for Azure SQL Data Warehouse
        # see https://docs.microsoft.com/en-us/azure/sql-data-warehouse/sql-data-warehouse-get-started-connect-sqlcmd
        + ["-I"]
        + ["-s,", "-W", "-Q", command]
    )

    # execute
    sqlcmd_command_log = [c if c != creds.password else "[REDACTED]" for c in sqlcmd_command]
    logger.info(f"Executing SqlCmd command now... \nSqlCmd command is: {sqlcmd_command_log}")
    ret_code, output = run_cmd(sqlcmd_command, live_mode=False)
    if ret_code:
        raise BCPandasException(f"SqlCmd command failed with exit code {ret_code}")
    try:
        result = pd.read_csv(filepath_or_buffer=output, skiprows=[1], header="infer")
    except pd.errors.EmptyDataError:
        result = None
    return result


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
    """
    Adopted from https://github.com/titan550/bcpy/blob/master/bcpy/format_file_builder.py#L25
    """
    return (
        input_string.replace('"', '\\"')
        .replace("'", "\\'")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


def build_format_file(df, delimiter):
    """
    Creates the non-xml SQL format file. Puts 4 spaces between each section.
    See https://docs.microsoft.com/en-us/sql/relational-databases/import-export/non-xml-format-files-sql-server
    for the specification of the file.

    # TODO add params/options to control:
    #   - the char type (not just SQLCHAR),

    Parameters
    ----------
    df : pandas DataFrame
    delimiter : a valid delimiter character

    Returns
    -------
    A string containing the format file
    """
    _space = " " * 4
    format_file_str = f"9.0\n{len(df.columns)}\n"  # Version and Number of columns
    for col_num, col_name in enumerate(df.columns, start=1):
        # last col gets a newline sep
        _delim = delimiter if col_num != len(df.columns) else NEWLINE
        _line = _space.join(
            [
                str(col_num),  # Host file field order
                SQLCHAR,  # Host file data type
                str(0),  # Prefix length
                str(0),  # Host file data length
                f'"{_escape(_delim)}"',  # Terminator (see note below)
                str(col_num),  # Server column order
                str(col_name),  # Server column name, optional as long as not blank
                sql_collation,  # Column collation
                "\n",
            ]
        )
        format_file_str += _line
    # FYI very important to surround the Terminator with quotes, otherwise BCP fails with:
    # "Unexpected EOF encountered in BCP data-file". Hugely frustrating bug.
    return format_file_str


def run_cmd(cmd, live_mode=True):
    """
    Runs the given command. 
    
    If live_mode is enabled, prints STDOUT in real time, 
    prints STDERR when command is complete, and logs both STDOUT and STDERR.
    Otherwise, just runs the command, prints STDERR, and returns both the exit code and STDOUT

    Paramters
    ---------
    cmd : list of str
        The command to run, to be submitted to `subprocess.Popen()`
    live_mode : bool, default True
        If to enable live_mode

    Returns
    -------
    The exit code of the command, and STDOUT if live_mode is enabled
    """
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, encoding="utf-8", errors="utf-8")
    if live_mode:
        # live stream STDOUT
        while True:
            outs = proc.stdout.readline()
            if outs:
                print(outs, end="")
                logger.info(outs)
            if proc.poll() is not None and outs == "":
                break
    errs = proc.stderr.readlines()
    if errs:
        print(errs, end="")
        logger.error(errs)
    if live_mode:
        return proc.returncode
    else:
        return proc.returncode, proc.stdout
