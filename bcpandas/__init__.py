from .main import to_sql, read_sql, SqlCreds
from .utils import bcp, sqlcmd

import warnings

name = "bcpandas"
__version__ = "0.1"

# TODO run check here that BCP and SqlCmd are installed
