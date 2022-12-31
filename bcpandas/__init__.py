from subprocess import DEVNULL, run
import warnings

from bcpandas.main import SqlCreds, to_sql  # noqa: F401
from bcpandas.utils import bcp  # noqa: F401

name = "bcpandas"
__version__ = "2.0.0"


# BCP check
try:
    run(["bcp", "-v"], stdout=DEVNULL, stderr=DEVNULL, stdin=DEVNULL)
except FileNotFoundError:
    warnings.warn("BCP utility not installed or not found in PATH, bcpandas will not work!")

del run, DEVNULL, warnings
