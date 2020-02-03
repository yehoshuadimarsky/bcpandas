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

from .utils import (
    assume_not_all_delims_and_quotechars,
    df_hypo_dates,
    df_hypo_floats,
    df_hypo_ints,
    df_hypo_mixed,
    df_hypo_text,
    execute_sql_statement,
    prep_df_for_comparison,
)


def test_tosql_all_delims():
    assert 1 == 2


def test_tosql_all_quotechars():
    assert 1 == 2


def test_tosql_nan_inf():
    assert 1 == 2


def test_tosql_debug():
    assert 1 == 2


def test_tosql_batchsize():
    assert 1 == 2


def test_tosql_append_only_some_cols():
    assert 1 == 2


def test_tosql_nan_null_last_col():
    assert 1 == 2


def test_tosql_empty_df():
    assert 1 == 2


@pytest.mark.usefixtures("database")
class _BaseToSql:
    sql_type = "table"

    # per https://github.com/pytest-dev/pytest/issues/2618#issuecomment-318584202
    fixture_names = ("sql_creds", "pyodbc_creds")

    @pytest.fixture(autouse=True, scope="function")
    def auto_injector_fixture(self, request):
        for name in self.fixture_names:
            setattr(self, name, request.getfixturevalue(name))


class TestToSqlReplace(_BaseToSql):
    """

    """

    table_name = "lotr_tosql_replace"

    def _test_base(self, df, index):
        assume_not_all_delims_and_quotechars(df)
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=self.sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="replace",
        )
        actual = pd.read_sql_query(sql=f"SELECT * FROM {self.table_name}", con=self.pyodbc_creds)
        expected = prep_df_for_comparison(df=df, index=index)
        assert_frame_equal(expected, actual, check_column_type="equiv")

    def _test_exists(self, df, index):
        self._test_base(df, index)

    def _test_not_exists(self, df, index):
        execute_sql_statement(
            self.pyodbc_creds.engine, f"DROP TABLE IF EXISTS dbo.{self.table_name}"
        )
        self._test_base(df, index)

    @given(df=df_hypo_mixed, index=st.booleans())
    @settings(deadline=None)
    def test_df_mixed(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_text, index=st.booleans())
    @settings(deadline=None)
    def test_df_text(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_ints, index=st.booleans())
    @settings(deadline=None)
    def test_df_ints(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_floats, index=st.booleans())
    @settings(deadline=None)
    def test_df_floats(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_dates, index=st.booleans())
    @settings(deadline=None)
    def test_df_dates(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)


class TestToSqlAppend(_BaseToSql):
    """

    """

    table_name = "lotr_tosql_append"

    def _test_exists(self, df, index):
        assume_not_all_delims_and_quotechars(df)
        # first populate the data
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=self.sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="replace",
        )

        # then test to sql with option of 'append'
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=self.sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="append",
        )
        actual = pd.read_sql_query(
            sql=f"SELECT * FROM dbo.{self.table_name}", con=self.pyodbc_creds
        )
        df = prep_df_for_comparison(df=df, index=index)
        expected = pd.concat([df, df], axis=0, ignore_index=True)  # appended
        assert_frame_equal(expected, actual, check_column_type="equiv")

    def _test_not_exists(self, df, index):
        execute_sql_statement(
            self.pyodbc_creds.engine, f"DROP TABLE IF EXISTS dbo.{self.table_name}"
        )
        assume_not_all_delims_and_quotechars(df)
        # test to sql with option of 'append'
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=self.sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="append",
        )
        actual = pd.read_sql_query(
            sql=f"SELECT * FROM dbo.{self.table_name}", con=self.pyodbc_creds
        )
        expected = prep_df_for_comparison(df=df, index=index)
        assert_frame_equal(expected, actual, check_column_type="equiv")

    @given(df=df_hypo_mixed, index=st.booleans())
    @settings(deadline=None)
    def test_df_mixed(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_text, index=st.booleans())
    @settings(deadline=None)
    def test_df_text(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_ints, index=st.booleans())
    @settings(deadline=None)
    def test_df_ints(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_floats, index=st.booleans())
    @settings(deadline=None)
    def test_df_floats(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_dates, index=st.booleans())
    @settings(deadline=None)
    def test_df_dates(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)


class TestToSqlFail(_BaseToSql):
    """

    """

    table_name = "lotr_tosql_fail"

    def _test_exists(self, df, index):
        assume_not_all_delims_and_quotechars(df)
        # first populate the data
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=self.sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="replace",
        )

        # then test to sql with option of 'fail'
        with pytest.raises(BCPandasValueError):
            to_sql(
                df=df,
                table_name=self.table_name,
                creds=self.sql_creds,
                index=index,
                sql_type=self.sql_type,
                if_exists="fail",
            )

    def _test_not_exists(self, df, index):
        execute_sql_statement(
            self.pyodbc_creds.engine, f"DROP TABLE IF EXISTS dbo.{self.table_name}"
        )
        assume_not_all_delims_and_quotechars(df)
        to_sql(
            df=df,
            table_name=self.table_name,
            creds=self.sql_creds,
            index=index,
            sql_type=self.sql_type,
            if_exists="fail",
        )
        actual = pd.read_sql_query(sql=f"SELECT * FROM {self.table_name}", con=self.pyodbc_creds)
        expected = prep_df_for_comparison(df=df, index=index)
        assert_frame_equal(expected, actual, check_column_type="equiv")

    @given(df=df_hypo_mixed, index=st.booleans())
    @settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_df_mixed(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_text, index=st.booleans())
    @settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_df_text(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_ints, index=st.booleans())
    @settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_df_ints(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_floats, index=st.booleans())
    @settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_df_floats(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)

    @given(df=df_hypo_dates, index=st.booleans())
    @settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
    def test_df_dates(self, df, index):
        self._test_exists(df, index)
        self._test_not_exists(df, index)


class TestToSqlOther(_BaseToSql):
    """

    """

    table_name = "lotr_tosql_other"

    def _test_df_template(self, df, sql_creds, index):
        assume_not_all_delims_and_quotechars(df)
        with pytest.raises(AssertionError):
            to_sql(
                df=df,
                table_name=self.table_name,
                creds=self.sql_creds,
                index=index,
                sql_type=self.sql_type,
                if_exists="bad_arg",
            )

    @given(df=df_hypo_mixed, index=st.booleans())
    @settings(deadline=None)
    def test_df_mixed(self, df, sql_creds, index):
        self._test_df_template(df, sql_creds, index)

    @given(df=df_hypo_text, index=st.booleans())
    @settings(deadline=None)
    def test_df_text(self, df, sql_creds, index):
        self._test_df_template(df, sql_creds, index)

    @given(df=df_hypo_ints, index=st.booleans())
    @settings(deadline=None)
    def test_df_ints(self, df, sql_creds, index):
        self._test_df_template(df, sql_creds, index)

    @given(df=df_hypo_floats, index=st.booleans())
    @settings(deadline=None)
    def test_df_floats(self, df, sql_creds, index):
        self._test_df_template(df, sql_creds, index)

    @given(df=df_hypo_dates, index=st.booleans())
    @settings(deadline=None)
    def test_df_dates(self, df, sql_creds, index):
        self._test_df_template(df, sql_creds, index)
