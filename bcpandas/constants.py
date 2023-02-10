"""
Created on Sat Aug  3 23:20:19 2019

@author: ydima
"""

import os
import sys
from typing import List, Optional

import pandas as pd


class BCPandasException(Exception):
    def __init__(self, message: str, details: Optional[List[str]] = None):
        super().__init__(message)
        self.details = details or []


class BCPandasValueError(BCPandasException):
    pass


IS_WIN32 = sys.platform == "win32"

# BCP terms
IN = "in"
OUT = "out"
QUERYOUT = "queryout"
TABLE = "table"
VIEW = "view"
QUERY = "query"

DIRECTIONS = (IN, OUT, QUERYOUT)
SQL_TYPES = (TABLE, VIEW, QUERY)
IF_EXISTS_OPTIONS = ("append", "replace", "fail")


# Text settings
_DELIMITER_OPTIONS = (",", "|", "\t")
_QUOTECHAR_OPTIONS = ('"', "'", "`", "~")
NEWLINE = os.linesep

# settings for both BCP and pandas.read_csv for reading from SQL
# delimiter should be characters that NEVER appear in the source data in SQL, have to guess a good one
# note that in pandas.read_csv a delimiter longer than a single character is interpreted as a regex
# see https://docs.microsoft.com/en-us/sql/relational-databases/import-export/specify-field-and-row-terminators-sql-server
# and https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
read_data_settings = {"delimiter": "\\t", "newline": NEWLINE}

# BCP Format File terms
SQLCHAR = "SQLCHAR"
sql_collation = "SQL_Latin1_General_CP1_CI_AS"


error_msg = """Data contains all of the possible {typ} characters {opts},
cannot use BCP to import it. Replace one of the possible {typ} characters in
your data, or use another method besides bcpandas.

Further background:

https://docs.microsoft.com/en-us/sql/relational-databases/import-export/specify-field-and-row-terminators-sql-server#characters-supported-as-terminators
"""


def get_delimiter(df: pd.DataFrame) -> str:
    for delim in _DELIMITER_OPTIONS:
        if not df.applymap(lambda x: delim in x if isinstance(x, str) else False).any().any():
            return delim
    raise BCPandasValueError(error_msg.format(typ="delimiter", opts=_DELIMITER_OPTIONS))


def get_quotechar(df: pd.DataFrame) -> str:
    for qc in _QUOTECHAR_OPTIONS:
        if not df.applymap(lambda x: qc in x if isinstance(x, str) else False).any().any():
            return qc
    raise BCPandasValueError(error_msg.format(typ="quote", opts=_QUOTECHAR_OPTIONS))
