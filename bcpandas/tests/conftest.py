# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima
"""

import subprocess
import time
import urllib

from bcpandas import SqlCreds
import pytest
import sqlalchemy as sa

from .utils import execute_sql_statement

server = "127.0.0.1,1433"
_pwd = "MyBigSQLPassword!!!"
_db_name = "db_bcpandas"
_docker_startup = 25  # seconds to wait to give the container time to start

docker_mssql_linux = "mcr.microsoft.com/mssql/server"


def pytest_addoption(parser):
    parser.addoption("--mssql-docker-image", action="store")


@pytest.fixture(scope="session")
def docker_db(pytestconfig):
    docker_image = pytestconfig.getoption("--mssql-docker-image", default=None)
    if docker_image is None:
        # assume Linux containers
        docker_image = f"{docker_mssql_linux}:2017-latest"
    _name = "bcpandas-mssql-container"
    cmd_start_container = [
        "docker",
        "run",
        "-d",
        "-e",
        "ACCEPT_EULA=Y",
        "-e",
        f"SA_PASSWORD={_pwd}",
    ]
    if docker_image.startswith(docker_mssql_linux):
        cmd_start_container += [
            "-e",
            "MSSQL_PID=Express",
        ]
    cmd_start_container += [
        "-p",
        "1433:1433",
        "--name",
        _name,
        docker_image,
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
    execute_sql_statement(creds_master.engine, f"CREATE DATABASE {_db_name}")


@pytest.fixture(scope="session")
def sql_creds():
    creds = SqlCreds(server=server, database=_db_name, username="sa", password=_pwd)
    return creds


@pytest.fixture(scope="session")
def pyodbc_creds(database):
    db_url = (
        "Driver={ODBC Driver 17 for SQL Server};"
        + f"Server={server};"
        + f"Database={_db_name};UID=sa;PWD={_pwd};"
    )
    engine = sa.engine.create_engine(
        f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(db_url)}"
    )
    return engine


def test_logins(sql_creds, pyodbc_creds):
    sql_creds.engine.connect()
    pyodbc_creds.connect()
