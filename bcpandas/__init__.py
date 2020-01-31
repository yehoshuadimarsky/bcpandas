from .main import to_sql, read_sql, SqlCreds
from .utils import bcp

from subprocess import run, DEVNULL
import warnings

name = "bcpandas"
__version__ = "0.2.6"


# BCP check
try:
    run(["bcp", "-v"], stdout=DEVNULL, stderr=DEVNULL)
except FileNotFoundError:
    warnings.warn("BCP utility not installed or not found in PATH, bcpandas will not work!")

del run, DEVNULL, warnings
