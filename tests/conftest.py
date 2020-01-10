# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima
"""

import subprocess
import time
import urllib

import numpy as np
import pandas as pd
import pytest
import pyodbc
import sqlalchemy as sa

from bcpandas import SqlCreds, sqlcmd


_pwd = "MyBigSQLPassword!!!"
_db_name = "db_bcpandas"
_docker_startup = 10  # seconds to wait to give the container time to start


@pytest.fixture(scope="session")
def docker_db():
    _name = "bcpandas-container"
    cmd_start_container = [
        "docker",
        "run",
        "-d",
        "-e",
        "ACCEPT_EULA=Y",
        "-e",
        f"SA_PASSWORD={_pwd}",
        "-e",
        "MSSQL_PID=Express",
        "-p",
        "1433:1433",
        "--name",
        _name,
        "mcr.microsoft.com/mssql/server:2017-latest",
    ]
    subprocess.run(cmd_start_container)
    time.sleep(_docker_startup)
    print("successfully started DB in docker...")
    yield
    print("Stopping container")
    subprocess.run(["docker", "stop", _name])
    print("Deleting container")
    subprocess.run(["docker", "rm", _name])
    print("all done!")


@pytest.fixture(scope="session")
def sql_creds():
    creds = SqlCreds(server="127.0.0.1,1433", database=_db_name, username="sa", password=_pwd)
    return creds


@pytest.fixture(scope="session")
def database(docker_db):
    creds_master = SqlCreds(
        server="127.0.0.1,1433", database="master", username="sa", password=_pwd
    )
    sqlcmd(creds_master, f"CREATE DATABASE {_db_name}")


@pytest.fixture(scope="session")
def pyodbc_creds(docker_db):
    db_url = (
        "Driver={ODBC Driver 17 for SQL Server};Server=127.0.0.1,1433;"
        + f"Database={_db_name};UID=sa;PWD={_pwd};"
    )
    engine = sa.engine.create_engine(
        f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(db_url)}"
    )
    return engine
