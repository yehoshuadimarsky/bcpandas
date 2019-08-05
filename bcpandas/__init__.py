from .main import to_sql, read_sql, SqlCreds

import warnings

name = "bcpandas"
__version__ = "0.1"


try:
    import pyodbc
except ImportError:
    warnings.warn("pyodbc library not installed, it is required for `read_sql`")

# TODO run check here that BCP and SqlCmd are installed
