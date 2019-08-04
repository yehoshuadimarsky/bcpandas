# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:20:19 2019

@author: ydima
"""

import os

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
DELIMITER = ","
QUOTECHAR = '"'
NEWLINE = os.linesep

# BCP Format File terms
SQLCHAR = "SQLCHAR"
sql_collation = "SQL_Latin1_General_CP1_CI_AS"
