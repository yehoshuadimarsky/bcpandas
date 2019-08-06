from .main import to_sql, read_sql, SqlCreds
from .utils import bcp, sqlcmd

import subprocess
import warnings

name = "bcpandas"
__version__ = "0.1.0"


# BCP check
cmds = [['bcp','-v'], ['sqlcmd','-?']]
for cmd in cmds:
    res = subprocess.run(cmd, stdout=subprocess.PIPE)
    if res.returncode:
        warnings.warn(f"{cmd[0].upper()} utility not installed or not found in PATH, bcpandas will not work!")

del subprocess, warnings, cmd, res
