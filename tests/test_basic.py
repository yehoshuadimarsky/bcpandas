# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima
"""

import pytest

import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import json
from bcpandas import SqlCreds, to_sql, read_sql, bcp, sqlcmd
from bcpandas.utils import _get_sql_create_statement


@pytest.fixture(scope="session")
def sql_creds():
    with open("../creds.json") as jf:
        _creds = json.load(jf)
    creds = SqlCreds(**_creds)
    return creds


@pytest.mark.parametrize(
    "if_exists",
    [
        "replace",
        "append",
        pytest.param("fail", marks=pytest.mark.xfail),
        pytest.param("the one ring", marks=pytest.mark.xfail),
    ],
)
def test_tosql_basic(sql_creds, if_exists):
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


@pytest.mark.skip("too big")
def test_big(sql_creds):
    df = pd.DataFrame(
        data=np.ndarray(shape=(100000, 6), dtype=float), columns=[f"col_{x}" for x in range(6)]
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

    expected = read_sql(
        "lotr_readsql",
        creds=sql_creds,
        sql_type="table",
        schema="dbo",
        mssql_odbc_driver_version=17,
    )

    assert_frame_equal(df, expected)
