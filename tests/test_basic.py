# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima
"""

import subprocess
import time

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from bcpandas import SqlCreds, bcp, read_sql, sqlcmd, to_sql
from bcpandas.utils import _get_sql_create_statement

_pwd = "MyBigSQLPassword!!!"
_db_name = "db_bcpandas"


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
    time.sleep(15)  # give the container time to start
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
def setup_db_tables(docker_db):
    creds_master = SqlCreds(
        server="127.0.0.1,1433", database="master", username="sa", password=_pwd
    )
    sqlcmd(creds_master, f"CREATE DATABASE {_db_name}")


@pytest.mark.parametrize(
    "if_exists",
    [
        "replace",
        "append",
        pytest.param("fail", marks=pytest.mark.xfail),
        pytest.param("the one ring", marks=pytest.mark.xfail),
    ],
)
def test_tosql_basic(sql_creds, setup_db_tables, if_exists):
    df = pd.DataFrame(
        {
            "col1": ["Sam, and", "Frodo", "Merry"],  # comma in first item
            "col2": ["the ring", "Morder", "Smeagol"],
            "col3": ['"The Lord of the Rings"', "Gandalf", "Bilbo"],
            "col4": [x for x in range(2107, 2110)],
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
    expected = sqlcmd(creds=sql_creds, command="SELECT * FROM dbo.lotr_tosql")

    # check
    assert_frame_equal(
        df if if_exists != "append" else pd.concat([df, df], axis=0, ignore_index=True), expected
    )


def test_big(sql_creds, setup_db_tables):
    _num_cols = 10
    df = pd.DataFrame(
        data=np.random.rand(1_000_000, _num_cols), columns=[f"col_{x}" for x in range(_num_cols)]
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


@pytest.mark.skip("not with docker yet")
def test_readsql_basic(sql_creds):
    df = pd.DataFrame(
        {
            "col1": ["Sam and", "Frodo", "Merry"],  # no comma in first item
            "col2": ["the ring", "Morder", "Smeagol"],
            "col3": ["The Lord of the Rings", "Gandalf", "Bilbo"],
            "col4": [x for x in range(2107, 2110)],
        }
    )
    # create new table
    sqlcmd(creds=sql_creds, command=_get_sql_create_statement(df=df, table_name="lotr_readsql"))

    # insert rows
    stmt = f"INSERT INTO lotr_readsql ( {', '.join(x for x in df.columns)} ) VALUES "
    for row in df.values:
        _s = ", ".join(f"'{str(item)}'" for item in row)
        stmt += f"({_s}), "
    stmt = stmt[:-2] + ";"  # replace last \n and comma with semicolon
    sqlcmd(creds=sql_creds, command=stmt)

    expected = read_sql("lotr_readsql", creds=sql_creds, sql_type="table", schema="dbo")

    assert_frame_equal(df, expected)
