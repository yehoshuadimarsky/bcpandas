# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:20:19 2019

@author: ydima
"""

import os


class BCPandasException(Exception):
    pass


class BCPandasValueError(BCPandasException):
    pass


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
QUOTECHAR = '"'
NEWLINE = os.linesep

# BCP Format File terms
SQLCHAR = "SQLCHAR"
sql_collation = "SQL_Latin1_General_CP1_CI_AS"


def get_delimiter(df):
    for delim in _DELIMITER_OPTIONS:
        if not df.applymap(lambda x: delim in x if isinstance(x, str) else False).any().any():
            return delim
    raise BCPandasValueError(
        f"Data contains all of the possible delimiter characters ({_DELIMITER_OPTIONS}), "
        "cannot use BCP to import it. Replace one of the possible delimiter characters in "
        "your data, or use another method besides bcpandas. \nFurther background: \n "
        "https://docs.microsoft.com/en-us/sql/relational-databases/import-export/specify-field-and-row-terminators-sql-server#characters-supported-as-terminators"
    )
