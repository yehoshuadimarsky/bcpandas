# flake8: noqa F401
from subprocess import DEVNULL, run
import warnings

from .main import SqlCreds, to_sql
from .utils import bcp

name = "bcpandas"
__version__ = "1.0.0"


# BCP check
try:
    run(["bcp", "-v"], stdout=DEVNULL, stderr=DEVNULL)
except FileNotFoundError:
    warnings.warn("BCP utility not installed or not found in PATH, bcpandas will not work!")

del run, DEVNULL, warnings
