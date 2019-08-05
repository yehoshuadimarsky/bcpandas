# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima
"""

import pytest

import pandas as pd
import numpy as np
import json
from bcpandas import SqlCreds, to_sql


@pytest.fixture(scope="session")
def sql_creds():
    with open("../creds.json") as jf:
        _creds = json.load(jf)
    creds = SqlCreds(**_creds)
    return creds


def test_basic(sql_creds):
    df = pd.DataFrame(
        {
            "col1": ["Sam, and", "Frodo", "Merry"],
            "col2": ["the ring", "Morder", "Smeagol"],
            "col3": ['"The Lord of the Rings"', "Gandalf", "Bilbo"],
            "col4": [x for x in range(2107, 2110)],
        }
    )
    to_sql(
        df=df,
        table_name="lotr1",
        creds=sql_creds,
        index=False,
        sql_type="table",
        if_exists="replace",
    )

    assert 1 == 1


def test_big(sql_creds):
    df = pd.DataFrame(
        data=np.ndarray(shape=(1_000_000, 6), dtype=float), columns=[f"col_{x}" for x in range(6)]
    )
    to_sql(
        df=df,
        table_name="test_floats_lots",
        creds=sql_creds,
        index=False,
        sql_type="table",
        if_exists="replace",
    )

    assert 1 == 1
