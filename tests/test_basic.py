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

from bcpandas import read_sql, sqlcmd, to_sql
from bcpandas.constants import BCPandasValueError


class TestToSqlBasic:
    """
    Uses the same table over and over, relying on the validity of the to_sql method with 'replace',
    which is the first test.

    In to_sql can only use a sql_type of 'table', so set that as variable to avoid misspellings.
    """

    table_name = "lotr_tosql"
    sql_type = "table"

    def test_tosql_replace(self, df_simple, df_tricky, sql_creds, database, pyodbc_creds):
        for df in (df_simple, df_tricky):
            to_sql(
                df=df,
                table_name=self.table_name,
                creds=sql_creds,
                index=False,
                sql_type=self.sql_type,
                if_exists="replace",
            )
            actual = pd.read_sql_query(sql="SELECT * FROM dbo.lotr_tosql", con=pyodbc_creds)
            assert_frame_equal(df, actual, check_column_type="equiv")

    def test_tosql_append(self, df_simple, df_tricky, sql_creds, database, pyodbc_creds):
        for df in (df_simple, df_tricky):
            # first populate the data
            to_sql(
                df=df,
                table_name=self.table_name,
                creds=sql_creds,
                index=False,
                sql_type=self.sql_type,
                if_exists="replace",
            )

            # then test to sql with option of 'append'
            to_sql(
                df=df,
                table_name=self.table_name,
                creds=sql_creds,
                index=False,
                sql_type=self.sql_type,
                if_exists="append",
            )
            actual = pd.read_sql_query(sql="SELECT * FROM dbo.lotr_tosql", con=pyodbc_creds)
            expected = pd.concat([df, df], axis=0, ignore_index=True)  # appended
            assert_frame_equal(expected, actual, check_column_type="equiv")

    def test_tosql_fail(self, df_simple, df_tricky, sql_creds, database, pyodbc_creds):
        for df in (df_simple, df_tricky):
            # first populate the data
            to_sql(
                df=df,
                table_name=self.table_name,
                creds=sql_creds,
                index=False,
                sql_type=self.sql_type,
                if_exists="replace",
            )

            # then test to sql with option of 'fail'
            with pytest.raises(BCPandasValueError):
                to_sql(
                    df=df,
                    table_name=self.table_name,
                    creds=sql_creds,
                    index=False,
                    sql_type=self.sql_type,
                    if_exists="fail",
                )

    def test_tosql_other(self, df_simple, df_tricky, sql_creds, database, pyodbc_creds):
        for df in (df_simple, df_tricky):
            with pytest.raises(AssertionError):
                to_sql(
                    df=df,
                    table_name=self.table_name,
                    creds=sql_creds,
                    index=False,
                    sql_type=self.sql_type,
                    if_exists="bad_arg",
                )


@pytest.mark.skip(reason="old")
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


@pytest.mark.skip(reason="too big")
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


@pytest.mark.skip()
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


@pytest.mark.skip()
def test_readsql_null_last_col(sql_creds, setup_db_tables, pyodbc_creds):
    df = pd.DataFrame(
        {
            "col1": ["Sam", "Frodo", None],  # NULL in last item
            "col2": ["the ring", "Morder", "Smeagol"],
            "col3": ["The Lord of the Rings", "Gandalf", "Bilbo"],
            "col4": [2107, 2108, np.NaN],
            "col5": [1.5, 2.5, np.NaN],
        }
    )

    # insert rows
    df.to_sql(
        name="lotr_read_null_last_col",
        con=pyodbc_creds,
        if_exists="replace",
        index=False,
        schema="dbo",
    )
    # get expected
    expected = read_sql("lotr_read_null_last_col", creds=sql_creds, sql_type="table", schema="dbo")
    # check
    assert_frame_equal(df, expected)


@pytest.mark.skip()
@pytest.mark.parametrize(
    "if_exists",
    [
        "replace",
        "append",
        pytest.param("fail", marks=pytest.mark.xfail(raises=BCPandasValueError)),
        pytest.param("the one ring", marks=pytest.mark.xfail(raises=AssertionError)),
    ],
)
def test_tosql_null_last_col(sql_creds, setup_db_tables, pyodbc_creds, if_exists):
    df = pd.DataFrame(
        {
            "col1": ["Sam and", "Frodo", None],
            "col2": ["the ring", "Morder", "Smeagol"],
            "col3": ["The Lord of the Rings", "Gandalf", "Bilbo"],
            "col4": [2107, 2108, np.NaN],  # integers
            "col5": [1.5, 2.5, np.NaN],  # floats
        }
    )
    # to sql
    to_sql(
        df=df,
        table_name="lotr_write_null_last_col",
        creds=sql_creds,
        index=False,
        sql_type="table",
        if_exists=if_exists,
    )
    # get expected
    expected = pd.read_sql_query(
        sql="SELECT * FROM dbo.lotr_write_null_last_col", con=pyodbc_creds
    ).astype({"col4": pd.np.float64, "col5": pd.np.float64})

    # check
    assert_frame_equal(
        df if if_exists != "append" else pd.concat([df, df], axis=0, ignore_index=True),
        expected,
        check_column_type="equiv",
    )
