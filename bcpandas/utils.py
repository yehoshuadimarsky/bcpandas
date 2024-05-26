"""
Created on Sat Aug  3 23:07:15 2019

@author: ydima
"""

import logging
import sys
from pathlib import Path
import random
import shlex
import string
from subprocess import PIPE, STDOUT, Popen
import tempfile
from typing import Dict, List, Optional, Tuple, Union
from re import sub

import pandas as pd

from bcpandas.constants import (
    DIRECTIONS,
    IN,
    IS_WIN32,
    NEWLINE,
    OUT,
    QUERY,
    QUERYOUT,
    SQLCHAR,
    TABLE,
    VIEW,
    BCPandasException,
    BCPandasValueError,
    read_data_settings,
    sql_collation,
)

logger = logging.getLogger(__name__)


def bcp(
    sql_item: str,
    direction: str,
    flat_file: Path,
    creds,
    print_output: bool,
    sql_type: str = "table",
    schema: str = "dbo",
    format_file_path: Optional[Path] = None,
    batch_size: Optional[int] = None,
    use_tablock: bool = False,
    col_delimiter: Optional[str] = None,
    row_terminator: Optional[str] = None,
    bcp_path: Optional[Union[str, Path]] = None,
    identity_insert: bool = False,
):
    """
    See https://docs.microsoft.com/en-us/sql/tools/bcp-utility
    """
    combos = {TABLE: [IN, OUT], QUERY: [QUERYOUT], VIEW: [IN, OUT]}
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
        auth = ["-U", quote_this(creds.username), "-P", quote_this(creds.password)]
    if creds.odbc_kwargs:
        kwargs = {k.lower(): v for k, v in creds.odbc_kwargs.items()}
        false_values = ("n", "no", "f", "false", "off", "0")

        if sys.platform != "win32" and "encrypt" in kwargs:
            auth += [f"-Y{'m' if kwargs['encrypt'] not in false_values else 'o'}"]

    # prepare SQL item string
    if sql_type == QUERY:
        # remove newlines for queries, otherwise messes up BCP
        sql_item_string = quote_this("".join(sql_item.splitlines()))
    else:
        sql_item_string = f"{schema}.{sql_item}"

    if creds.port is not None and creds.port != 1433:
        server = f"{creds.server},{creds.port}"
    else:
        server = creds.server

    # construct BCP command
    bcp_command = [
        "bcp" if bcp_path is None else quote_this(str(bcp_path)),
        sql_item_string,
        direc,
        str(flat_file),
        "-S",
        server,
        "-d",
        creds.database,
        "-q",  # Executes the SET QUOTED_IDENTIFIERS ON statement, needed for Azure SQL DW
    ] + auth

    if batch_size:
        bcp_command += ["-b", str(batch_size)]

    if use_tablock:
        bcp_command += ["-h", "TABLOCK"]

    if identity_insert:
        bcp_command += ["-E"]

    # formats
    if direc == IN and format_file_path is not None:
        bcp_command += ["-f", str(format_file_path)]
    elif direc in (OUT, QUERYOUT):
        bcp_command += [
            "-c",  # marking as character data, not Unicode (maybe make as param?)
            quote_this(
                f"-t{read_data_settings['delimiter'] if col_delimiter is None else col_delimiter}"
            ),
            quote_this(
                f"-r{read_data_settings['newline'] if row_terminator is None else row_terminator}"
            ),
        ]

    # execute
    bcp_command_log = ", ".join(bcp_command)
    bcp_command_log_msg = sub(r"-P,\s.*,", "-P, [REDACTED],", bcp_command_log)
    logger.info(f"Executing BCP command now... \nBCP command is: {bcp_command_log_msg}")
    ret_code, output = run_cmd(bcp_command, print_output=print_output)
    if ret_code != 0:
        raise BCPandasException(
            f"Bcp command failed with exit code {ret_code}",
            details=[line for line in output if line.startswith("Error =")],
        )


def get_temp_file(directory: Optional[Path] = None) -> Path:
    """
    Returns full path to a temporary file without creating it.
    """
    if directory is None:
        tmp_dir = Path(tempfile.gettempdir())
    else:
        tmp_dir = directory
    return tmp_dir / "".join(random.choices(string.ascii_letters + string.digits, k=21))


def _escape(input_string: str) -> str:
    """
    Adopted from https://github.com/titan550/bcpy/blob/master/bcpy/format_file_builder.py#L25
    """
    return (
        input_string.replace('"', '\\"')
        .replace("'", "\\'")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


def build_format_file(
    df: pd.DataFrame,
    delimiter: str,
    db_cols_order: Optional[Dict[str, int]] = None,
    collation: str = sql_collation,
) -> str:
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
    db_cols_order : dict, optional
        Dict of {database column name -> ordinal position of the column}.
        Maps existing columns in the database to their ordinal position, i.e. the order of the columns in the db table.
        1-indexed, so the first columns is 1, second is 2, etc.
        Only needed if the order of the columns in the dataframe doesn't match the database.
    collation: str, optional
        Collation to be used in the format file. The default value is 'SQL_Latin1_General_CP1_CI_AS'

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
                str(
                    col_num if not db_cols_order else db_cols_order[str(col_name)]
                ),  # Server column order
                str(col_name).replace(
                    " ", r"\s"
                ),  # Server column name, optional as long as not blank
                collation,  # Column collation
                "\n",
            ]
        )
        format_file_str += _line
    # FYI very important to surround the Terminator with quotes, otherwise BCP fails with:
    # "Unexpected EOF encountered in BCP data-file". Hugely frustrating bug.
    return format_file_str


def quote_this(this: str, skip: bool = False) -> str:
    """
    OS-safe way to quote a string.

    Returns the string with quotes around it.
    On Windows ~~it's double quotes~~ we skip quoting,
    on Linux it's single quotes.
    """
    if isinstance(this, str):
        if IS_WIN32:
            return this  # TODO maybe change?
        else:
            return shlex.quote(this)
    else:
        return this


def run_cmd(cmd: List[str], *, print_output: bool) -> Tuple[int, List[str]]:
    """
    Runs the given command.

    Prints STDOUT in real time,  prints STDERR when command is complete,
    and logs both STDOUT and STDERR.

    Paramters
    ---------
    cmd : list of str
        The command to run, to be submitted to `subprocess.Popen()`
    print_output: bool
        Whether to print output to STDOUT in real time.
        Regardless, the output will be logged.

    Returns
    -------
    The exit code of the command and all of its output.
    """
    if IS_WIN32:
        with_shell = False
    else:
        with_shell = True
        cmd = " ".join(cmd).replace("\\", "\\\\")  # type: ignore
    proc = Popen(
        cmd,
        stdout=PIPE,
        stderr=STDOUT,
        encoding="utf-8",
        errors="utf-8",
        shell=with_shell,
    )
    stdout = []
    # live stream STDOUT and STDERR
    while True:
        outs = proc.stdout.readline()  # type: ignore[union-attr]
        if outs:
            if print_output:
                print(outs, end="")
            logger.info(outs)
            stdout.append(outs)
        if proc.poll() is not None and outs == "":
            break
    return proc.returncode, stdout
