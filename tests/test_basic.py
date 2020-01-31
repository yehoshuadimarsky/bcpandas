# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima


There are 2 categories of tests we want to do:
    - Test every code path, i.e. every combination of arguments for each function
    - Test with different datasets that have different properties

"""

import subprocess
import time
import urllib

import hypothesis.strategies as st
import numpy as np
import pandas as pd
import pytest
import sqlalchemy as sa
from hypothesis import given, note, settings, assume
from hypothesis.extra import pandas as hpd
from pandas.testing import assert_frame_equal

import pyodbc
from bcpandas import read_sql, to_sql
from bcpandas.constants import (
    BCPandasValueError,
    read_data_settings,
    _DELIMITER_OPTIONS,
    _QUOTECHAR_OPTIONS,
)


# Hypo - typical use cases
#   - DataFrame: at least one row
#   - Text: All text in ASCII 32-127, except the space character (32)
#   - Integers: between -2**31-1 and 2**31-1
#   - Floats: between -2**31-1 and 2**31-1, without NaN or inf

MAX_VAL = 2 ** 31 - 1

text_basic_strat = st.text(alphabet=st.characters(min_codepoint=33, max_codepoint=127), min_size=1)


hypo_df = hpd.data_frames(
    columns=[
        hpd.column(name="col1", elements=text_basic_strat),
        hpd.column(name="col2", elements=st.integers(min_value=-MAX_VAL, max_value=MAX_VAL)),
        hpd.column(
            name="col3",
            elements=st.floats(
                min_value=-MAX_VAL, max_value=MAX_VAL, allow_nan=False, allow_infinity=False
            ),
        ),
    ],
    index=hpd.range_indexes(min_size=1),
)


def not_has_all_delims(df):
    return not all(
        df.applymap(lambda x: delim in x if isinstance(x, str) else False).any().any()
        for delim in _DELIMITER_OPTIONS
    )


def not_has_all_quotechars(df):
    return not all(
        df.applymap(lambda x: qc in x if isinstance(x, str) else False).any().any()
        for qc in _QUOTECHAR_OPTIONS
    )


# TODO
# Test space character, empty string, NaN, and Inf

# datasets
# can't use fixtures, as can't pass fixtures to @pytest.mark.parametrize()
df_simple = pd.DataFrame(
    {
        "col1": ["Sam and", "Frodo", "Merry"],
        "col2": ["the ring", "Mordor", "Smeagol"],
        "col3": ["The Lord of the Rings", "Gandalf", "Bilbo"],
        "col4": [2107, 2108, 2109],  # integers
        "col5": [1.5, 2.5, 3.5],  # floats
    }
)


class TestToSqlBasic:
    """
    For all tests, the 'actual' is retrieved using the built-in pandas methods, to compare to the 
    'expected' which used bcpandas.

    Uses the same table over and over, relying on the validity of the to_sql method with 'replace',
    which is the first test.

    In to_sql can only use a sql_type of 'table', so set that as variable to avoid misspellings.
    """

    table_name = "lotr_tosql"
    sql_type = "table"

    @given(df=hypo_df)
    @settings(deadline=None)
    def test_tosql_replace(self, df, sql_creds, database, pyodbc_creds):
        assume(not_has_all_delims(df))
        assume(not_has_all_quotechars(df))
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=sql_creds,
            index=False,
            sql_type=self.sql_type,
            if_exists="replace",
        )
        actual = pd.read_sql_query(sql=f"SELECT * FROM {self.table_name}", con=pyodbc_creds)
        assert_frame_equal(df, actual, check_column_type="equiv")

    @given(df=hypo_df)
    @settings(deadline=None)
    def test_tosql_append(self, df, sql_creds, database, pyodbc_creds):
        assume(not_has_all_delims(df))
        assume(not_has_all_quotechars(df))
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

    @given(df=hypo_df)
    @settings(deadline=None)
    def test_tosql_fail(self, df, sql_creds, database, pyodbc_creds):
        assume(not_has_all_delims(df))
        assume(not_has_all_quotechars(df))
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

    @given(df=hypo_df)
    @settings(deadline=None)
    def test_tosql_other(self, df, sql_creds, database, pyodbc_creds):
        assume(not_has_all_delims(df))
        assume(not_has_all_quotechars(df))
        with pytest.raises(AssertionError):
            to_sql(
                df=df,
                table_name=self.table_name,
                creds=sql_creds,
                index=False,
                sql_type=self.sql_type,
                if_exists="bad_arg",
            )


class TestReadSqlBasic:
    """
    For all tests, the 'actual' is retrieved using the built-in pandas methods, to compare to the 
    'expected' which used bcpandas.

    Because dtypes change when reading from text files, ignoring dtypes checks. TODO how to really fix this
    """

    table_name = "lotr_readsql"
    view_name = f"v_{table_name}"

    @given(df=hypo_df)
    @settings(deadline=None)
    def test_readsql_table(self, df, sql_creds, database, pyodbc_creds):
        assume(not_has_all_delims(df))
        assume(not_has_all_quotechars(df))
        # create table and insert rows
        df.to_sql(
            name=self.table_name, con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
        )
        # get expected
        expected = read_sql(self.table_name, creds=sql_creds, sql_type="table", schema="dbo")
        # check
        assert_frame_equal(df, expected, check_dtype=False)

    @given(df=hypo_df)
    @settings(deadline=None)
    def test_readsql_view(self, df, sql_creds, database, pyodbc_creds):
        assume(not_has_all_delims(df))
        assume(not_has_all_quotechars(df))
        # create table and insert rows
        df.to_sql(
            name=self.table_name, con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
        )
        # create corresponding view
        conn = pyodbc.connect(pyodbc_creds.engine.url.query["odbc_connect"], autocommit=True)
        conn.execute(f"DROP VIEW IF EXISTS dbo.{self.view_name}")
        conn.execute(f"CREATE VIEW dbo.{self.view_name} AS SELECT * FROM dbo.{self.table_name}")
        conn.close()

        # get expected
        expected = read_sql(self.view_name, creds=sql_creds, sql_type="view", schema="dbo")
        # check
        assert_frame_equal(df, expected, check_dtype=False)

    @given(df=hypo_df)
    @settings(deadline=None)
    def test_readsql_query(self, df, sql_creds, database, pyodbc_creds):
        assume(not_has_all_delims(df))
        assume(not_has_all_quotechars(df))
        # create table and insert rows
        df.to_sql(
            name=self.table_name, con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
        )
        # get expected
        expected = read_sql(
            f"SELECT * FROM {self.table_name}", creds=sql_creds, sql_type="query", schema="dbo"
        )
        # check
        assert_frame_equal(df, expected, check_dtype=False)

    def test_readsql_custom_delimiter(self, sql_creds, database, pyodbc_creds):
        df = pd.DataFrame(
            {
                "col1": ["Sam and <", "Frodo", "Merry"],
                "col4": [2107, 2108, 2109],  # integers
                "col5": [1.5, 2.5, 3],  # floats
            }
        )
        # create table and insert rows
        df.to_sql(
            name=self.table_name, con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
        )
        # get expected
        expected = read_sql(
            self.table_name, creds=sql_creds, sql_type="table", schema="dbo", delimiter="|"
        )
        # check
        assert_frame_equal(df, expected)

        # check that correctly finds the error if bad cust delim is passed
        with pytest.raises(BCPandasValueError):
            read_sql(
                self.table_name, creds=sql_creds, sql_type="table", schema="dbo", delimiter="<"
            )

    def test_readsql_bad_delimiter(self, sql_creds, database, pyodbc_creds):
        # get default delimiter
        delim_default = read_data_settings["delimiter"]
        # has comma in data field, which is also the default delimiter
        df = pd.DataFrame(
            {
                "col1": [f"Sam and {delim_default}", "Frodo", "Merry"],
                "col4": [2107, 2108, 2109],  # integers
            }
        )
        # create table and insert rows
        df.to_sql(
            name=self.table_name, con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
        )
        # check that correctly finds the error
        with pytest.raises(BCPandasValueError):
            read_sql(self.table_name, creds=sql_creds, sql_type="table", schema="dbo")

        # check other error occurs if don't check_delim
        # TODO which error does it raise?
        expected = read_sql(
            self.table_name, creds=sql_creds, sql_type="table", schema="dbo", check_delim=False
        )
        assert_frame_equal(df, expected)


# ------


@given(index=st.booleans(), debug=st.booleans(), batch_size=st.integers(min_value=1, max_value=3))
@settings(deadline=None)
def test_tosql_switches(sql_creds, database, pyodbc_creds, index, debug, batch_size):
    to_sql(
        df=df_simple,
        table_name="lotr_tosql_switches",
        creds=sql_creds,
        index=index,
        sql_type="table",
        if_exists="replace",
        debug=debug,
        batch_size=batch_size,
    )
    actual = pd.read_sql_query(sql="SELECT * FROM dbo.lotr_tosql_switches", con=pyodbc_creds)
    assert_frame_equal(df_simple, actual, check_column_type="equiv")
