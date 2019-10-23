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

from bcpandas import SqlCreds, bcp, read_sql, sqlcmd, to_sql

_pwd = "MyBigSQLPassword!!!"
_db_name = "db_bcpandas"
_docker_startup = 10  # seconds to wait to give the container time to start


@pytest.fixture(scope="module")
def docker_db():
    _name = "bcpandas-container-perf"
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
def creds_and_setup(docker_db):
    # creds
    creds = SqlCreds(server="127.0.0.1,1433", database=_db_name, username="sa", password=_pwd)

    # setup
    creds_master = SqlCreds(
        server="127.0.0.1,1433", database="master", username="sa", password=_pwd
    )
    sqlcmd(creds_master, f"CREATE DATABASE {_db_name}")

    return creds


@pytest.fixture(scope="module")
def data():

    from pandas.util import testing as pdt
    import random

    dikt = {}
    num_cols = 6
    num_rows = 100000

    dikt["ints"] = pdt.makeCustomDataframe(
        num_rows,
        num_cols,
        r_idx_type="i",
        data_gen_f=lambda r, c: np.random.randint(-250000, 250000),
    )

    dikt["floats"] = dikt["ints"] / 100000

    dikt["strings"] = pdt.makeCustomDataframe(num_rows, num_cols, r_idx_type="i")

    rand_str = ["Alpha", "Bravo", "Charlie", "Delta," "Echo", "Foxtrot"]
    rand_int = list(np.random.randint(-5, 5, size=(10)))
    rand_float = list(np.random.rand(10))

    dikt["mixed_on_rows"] = pdt.makeCustomDataframe(
        num_rows,
        num_cols,
        r_idx_type="i",
        data_gen_f=lambda r, c: random.choice(rand_str + rand_int + rand_float),
    )

    def _func_m(r, c):
        if c in (0, 3):
            return random.choice(rand_str)
        elif c in (1, 4):
            return random.choice(rand_int)
        elif c in (2, 5):
            return random.choice(rand_float)

    dikt["mixed_on_cols"] = pdt.makeCustomDataframe(
        num_rows, num_cols, r_idx_type="i", data_gen_f=_func_m
    )

    return dikt


def test_tosql_ints(creds_and_setup, data):
    pass


def test_readsql_bcp_ints(creds_and_setup, data, benchmark):
    # setup
    table_name = "perf_ints"
    to_sql(
        data["ints"], table_name=table_name, creds=creds_and_setup, index=False, if_exists="replace"
    )

    # benchmark!
    benchmark(read_sql, table_name="perf_ints", creds=creds_and_setup)


def test_readsql_pd_ints(creds_and_setup, data, benchmark):
    # setup
    table_name = "perf_ints"
    to_sql(
        data["ints"], table_name=table_name, creds=creds_and_setup, index=False, if_exists="replace"
    )

    # benchmark!
    benchmark(pd.read_sql_table, table_name=table_name, con=creds_and_setup.engine)
