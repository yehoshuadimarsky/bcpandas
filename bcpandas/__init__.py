# flake8: noqa F401
import warnings
from subprocess import DEVNULL, run

from .main import SqlCreds, read_sql, to_sql
from .utils import bcp

name = "bcpandas"
__version__ = "0.2.6"


# BCP check
try:
    run(["bcp", "-v"], stdout=DEVNULL, stderr=DEVNULL)
except FileNotFoundError:
    warnings.warn("BCP utility not installed or not found in PATH, bcpandas will not work!")

del run, DEVNULL, warnings
