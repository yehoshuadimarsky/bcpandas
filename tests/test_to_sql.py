# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima


There are 2 categories of tests we want to do:
    - Test every code path, i.e. every combination of arguments for each function
    - Test with different datasets that have different properties
"""

from bcpandas import to_sql
from bcpandas.constants import BCPandasValueError
from hypothesis import HealthCheck, given, settings
import hypothesis.strategies as st
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from .utils import assume_not_all_delims_and_quotechars, hypo_df


def test_tosql_all_delims():
    raise NotImplementedError()


def test_tosql_all_quotechars():
    raise NotImplementedError()


def test_tosql_nan_inf():
    raise NotImplementedError()


def test_tosql_debug():
    raise NotImplementedError()


def test_tosql_batchsize():
    raise NotImplementedError()


def test_tosql_append_only_some_cols():
    raise NotImplementedError()


def test_tosql_nan_null_last_col():
    raise NotImplementedError()


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

    @given(df=hypo_df, index=st.booleans())
    @settings(deadline=None)
    def test_tosql_replace(self, df, sql_creds, database, pyodbc_creds, index):
        assume_not_all_delims_and_quotechars(df)
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="replace",
        )
        actual = pd.read_sql_query(sql=f"SELECT * FROM {self.table_name}", con=pyodbc_creds)
        if index:
            df = df.reset_index()
        df = df.replace(
            {"": None}
        )  # Empty string becomes NULL in SQL (None in pandas), marking as ok for now
        assert_frame_equal(df, actual, check_column_type="equiv")

    @given(df=hypo_df, index=st.booleans())
    @settings(deadline=None)
    def test_tosql_append(self, df, sql_creds, database, pyodbc_creds, index):
        assume_not_all_delims_and_quotechars(df)
        # first populate the data
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="replace",
        )

        # then test to sql with option of 'append'
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="append",
        )
        actual = pd.read_sql_query(sql="SELECT * FROM dbo.lotr_tosql", con=pyodbc_creds)
        if index:
            df = df.reset_index()
        df = df.replace(
            {"": None}
        )  # Empty string becomes NULL in SQL (None in pandas), marking as ok for now
        expected = pd.concat([df, df], axis=0, ignore_index=True)  # appended
        assert_frame_equal(expected, actual, check_column_type="equiv")

    @given(df=hypo_df, index=st.booleans())
    @settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_tosql_fail(self, df, sql_creds, database, pyodbc_creds, index):
        assume_not_all_delims_and_quotechars(df)
        # first populate the data
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="replace",
        )

        # then test to sql with option of 'fail'
        with pytest.raises(BCPandasValueError):
            to_sql(
                df=df,
                table_name=self.table_name,
                creds=sql_creds,
                index=index,
                sql_type=self.sql_type,
                if_exists="fail",
            )

    @given(df=hypo_df, index=st.booleans())
    @settings(deadline=None)
    def test_tosql_other(self, df, sql_creds, database, pyodbc_creds, index):
        assume_not_all_delims_and_quotechars(df)
        with pytest.raises(AssertionError):
            to_sql(
                df=df,
                table_name=self.table_name,
                creds=sql_creds,
                index=index,
                sql_type=self.sql_type,
                if_exists="bad_arg",
            )
