# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima
"""

import time
import urllib

from bcpandas import SqlCreds
import pytest
import sqlalchemy as sa

from .utils import DockerDB

_db_name = "db_bcpandas"
_docker_startup = 15  # seconds to wait to give the container time to start
docker_db_obj = DockerDB(
    container_name="bcpandas-mssql-container", sa_sql_password="MyBigSQLPassword!!!"
)


def pytest_addoption(parser):
    parser.addoption("--mssql-docker-image", action="store")


@pytest.fixture(scope="session")
def docker_db(pytestconfig):
    # figure out which docker image
    docker_image = pytestconfig.getoption("--mssql-docker-image", default=None)
    if docker_image is not None:
        # overwrite default image of the DockerDB object
        docker_db_obj.mssql_image = docker_image

    # start it
    docker_db_obj.start()
    time.sleep(_docker_startup)
    print("successfully started DB in docker...")
    yield
    print("Stopping container")
    docker_db_obj.stop()
    print("Deleting container")
    docker_db_obj.remove()
    print("all done!")


@pytest.fixture(scope="session")
def database(docker_db):
    docker_db_obj.create_database(_db_name)


@pytest.fixture(scope="session")
def sql_creds():
    creds = SqlCreds(
        server=docker_db_obj.address,
        database=_db_name,
        username="sa",
        password=docker_db_obj.sa_sql_password,
    )
    return creds


@pytest.fixture(scope="session")
def pyodbc_creds(database):
    db_url = (
        "Driver={ODBC Driver 17 for SQL Server};"
        + f"Server={docker_db_obj.address};"
        + f"Database={_db_name};UID=sa;PWD={docker_db_obj.sa_sql_password};"
    )
    engine = sa.engine.create_engine(
        f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(db_url)}"
    )
    return engine
