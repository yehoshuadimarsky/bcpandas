from subprocess import DEVNULL, run
import warnings

from bcpandas.main import SqlCreds, to_sql
from bcpandas.utils import bcp

__version__ = "2.7.2"

# BCP check
try:
    run(["bcp", "-v"], stdout=DEVNULL, stderr=DEVNULL, stdin=DEVNULL)
except (FileNotFoundError, PermissionError):
    warnings.warn("BCP utility not installed or not found in PATH, bcpandas will not work!")

del run, DEVNULL, warnings

__all__ = ["SqlCreds", "to_sql", "bcp"]
