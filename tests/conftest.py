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

# if running the tests from docker using docker compose
USING_DOCKER = True

if USING_DOCKER:
    server = "db"  # the name in docker-compose
else:
    server = "127.0.0.1,1433"

_pwd = "MyBigSQLPassword!!!"
_db_name = "db_bcpandas"
_docker_startup = 10  # seconds to wait to give the container time to start


if USING_DOCKER:

    @pytest.fixture(scope="session")
    def docker_db():
        """
        Dummy func for when not inside docker
        """
        pass


else:

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
def database(docker_db):
    creds_master = SqlCreds(server=server, database="master", username="sa", password=_pwd)
    sqlcmd(creds_master, f"CREATE DATABASE {_db_name};")
    yield
    sqlcmd(creds_master, f"DROP DATABASE {_db_name};")


@pytest.fixture(scope="session")
def sql_creds():
    creds = SqlCreds(server=server, database=_db_name, username="sa", password=_pwd)
    return creds


@pytest.fixture(scope="session")
def pyodbc_creds(docker_db):
    db_url = (
        "Driver={ODBC Driver 17 for SQL Server};"
        + f"Server={server};"
        + f"Database={_db_name};UID=sa;PWD={_pwd};"
    )
    engine = sa.engine.create_engine(
        f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(db_url)}"
    )
    return engine
