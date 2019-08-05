# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima
"""

import pytest

import pandas as pd
import json
from bcpandas import SqlCreds, to_sql, read_sql


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
    to_sql(df=df, table_name="lotr1", creds=sql_creds, sql_type="table")

    assert 1 == 1
