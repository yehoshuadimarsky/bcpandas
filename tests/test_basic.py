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
from pandas.testing import assert_frame_equal

from bcpandas import SqlCreds, bcp, read_sql, sqlcmd, to_sql
from bcpandas.constants import BCPandasException, BCPandasValueError
from bcpandas.utils import _get_sql_create_statement

_pwd = "MyBigSQLPassword!!!"
_db_name = "db_bcpandas"
_docker_startup = 15  # seconds to wait to give the container time to start


@pytest.fixture(scope="module")
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


@pytest.fixture(scope="module")
def sql_creds():
    creds = SqlCreds(
        server="127.0.0.1,1433",
        database=_db_name,
        username="sa",
        password=_pwd,
        # Encrypt= "yes",
        # TrustServerCertificate="yes",
    )
    return creds


@pytest.fixture(scope="module")
def setup_db_tables(docker_db):
    creds_master = SqlCreds(
        server="127.0.0.1,1433", database="master", username="sa", password=_pwd
    )
    sqlcmd(creds_master, f"CREATE DATABASE {_db_name}")


@pytest.fixture(scope="module")
def pyodbc_creds(docker_db):

    db_url = (
        "Driver={ODBC Driver 17 for SQL Server};Server=127.0.0.1,1433;"
        + f"Database={_db_name};UID=sa;PWD={_pwd};"
    )
    engine = sa.engine.create_engine(
        f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(db_url)}"
    )
    return engine


@pytest.mark.parametrize(
    "if_exists",
    [
        "replace",
        "append",
        pytest.param("fail", marks=pytest.mark.xfail(raises=BCPandasValueError)),
        pytest.param("the one ring", marks=pytest.mark.xfail(raises=AssertionError)),
    ],
)
def test_tosql_basic(sql_creds, setup_db_tables, pyodbc_creds, if_exists):
    df = pd.DataFrame(
        {
            "col1": ["Sam and", "Frodo", "Merry"],
            "col2": ["the ring", "Morder", "Smeagol"],
            "col3": ["The Lord of the Rings", "Gandalf", "Bilbo"],
            "col4": [2107, 2108, 2109],  # integers
            "col5": [1.5, 2.5, 3.5],  # floats
        }
    )
    # to sql
    to_sql(
        df=df,
        table_name="lotr_tosql",
        creds=sql_creds,
        index=False,
        sql_type="table",
        if_exists=if_exists,
    )
    # get expected
    expected = pd.read_sql_query(sql="SELECT * FROM dbo.lotr_tosql", con=pyodbc_creds).astype(
        {"col4": pd.np.int64, "col5": pd.np.float64}
    )

    # check
    assert_frame_equal(
        df if if_exists != "append" else pd.concat([df, df], axis=0, ignore_index=True),
        expected,
        check_column_type="equiv",
    )


@pytest.mark.skip(reason="fails for now")
@pytest.mark.parametrize(
    "if_exists",
    [
        "replace",
        "append",
        pytest.param("fail", marks=pytest.mark.xfail(raises=BCPandasValueError)),
        pytest.param("the one ring", marks=pytest.mark.xfail(raises=AssertionError)),
    ],
)
def test_tosql_edgecases(sql_creds, setup_db_tables, pyodbc_creds, if_exists):
    df = pd.DataFrame(
        {
            "col1": ["Sam, and", "Frodo", "Merry"],  # comma in first item
            "col2": ["the ring", "Morder", "Smeagol"],
            "col3": ['"The Lord of the Rings"', "Gandalf", "Bilbo"],  # double quote in first item
            "col4": [2107, 2108, 2109],  # integers
            "col5": [1.5, 2.5, 3.5],  # floats
        }
    )
    # to sql
    to_sql(
        df=df,
        table_name="lotr_tosql2",
        creds=sql_creds,
        index=False,
        sql_type="table",
        if_exists=if_exists,
    )
    # get expected
    expected = pd.read_sql_query(sql="SELECT * FROM dbo.lotr_tosql2", con=pyodbc_creds)

    # check
    assert_frame_equal(
        df if if_exists != "append" else pd.concat([df, df], axis=0, ignore_index=True), expected
    )


@pytest.mark.skip()
def test_big(sql_creds, setup_db_tables):
    _num_cols = 10
    df = pd.DataFrame(
        data=np.random.rand(100_000, _num_cols), columns=[f"col_{x}" for x in range(_num_cols)]
    )
    # to sql
    to_sql(
        df=df,
        table_name="test_floats_lots",
        creds=sql_creds,
        index=False,
        sql_type="table",
        if_exists="replace",
    )
    # get expected
    expected = sqlcmd(creds=sql_creds, command="SELECT * FROM dbo.test_floats_lots")
    # check
    assert_frame_equal(df, expected)


def test_readsql_basic(sql_creds, setup_db_tables, pyodbc_creds):
    df = pd.DataFrame(
        {
            "col1": ["Sam and", "Frodo", "Merry"],
            "col2": ["the ring", "Morder", "Smeagol"],
            "col3": ["The Lord of the Rings", "Gandalf", "Bilbo"],
            "col4": [2107, 2108, 2109],  # integers
            "col5": [1.5, 2.5, 3.5],  # floats
        }
    )

    # insert rows
    df.to_sql(
        name="lotr_readsql1", con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
    )
    # get expected
    expected = read_sql("lotr_readsql1", creds=sql_creds, sql_type="table", schema="dbo")
    # check
    assert_frame_equal(df, expected)


@pytest.mark.skip(reason="fails for now")
def test_readsql_edgecases(sql_creds, setup_db_tables, pyodbc_creds):
    df = pd.DataFrame(
        {
            "col1": ["Sam, and", "Frodo", "Merry"],  # comma in first item
            "col2": ["the ring", "Morder", "Smeagol"],
            "col3": ['"The Lord of the Rings"', "Gandalf", "Bilbo"],  # double quote in first item
            "col4": [2107, 2108, 2109],  # integers
            "col5": [1.5, 2.5, 3.5],  # floats
        }
    )

    # insert rows
    df.to_sql(
        name="lotr_readsql2", con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
    )
    # get expected
    expected = read_sql("lotr_readsql2", creds=sql_creds, sql_type="table", schema="dbo")
    # check
    assert_frame_equal(df, expected)
