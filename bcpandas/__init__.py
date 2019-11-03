from .main import to_sql, read_sql, SqlCreds
from .utils import bcp, sqlcmd

from subprocess import run, DEVNULL
import warnings

name = "bcpandas"
__version__ = "0.2.0"


# BCP check
cmds = [["bcp", "-v"], ["sqlcmd", "-?"]]
for cmd in cmds:
    try:
        run(cmd, stdout=DEVNULL, stderr=DEVNULL)
    except FileNotFoundError:
        warnings.warn(
            f"{cmd[0].upper()} utility not installed or not found in PATH, bcpandas will not work!"
        )

del run, DEVNULL, warnings, cmd
