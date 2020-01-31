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

from bcpandas import read_sql, to_sql
from bcpandas.constants import BCPandasValueError, read_data_settings


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


df_tricky = pd.DataFrame(
    {
        "col1": ["Sam and,", "Frodo", "Merry"],
        "col2": ["the, ring", 'this is "Mordor"', "Smeagol"],
        "col3": ["The Lord 'of' the Rings", "Gandalf`", "Bilbo"],
        "col4": [2107, 2108, 2109],  # integers
        "col5": [1.5, 2.5, 3],  # floats
    }
)


# TODO this causes failures with type mismatches in the empty string, to fix
df_null_last_col = pd.DataFrame(
    {
        "col1": ["Sam", "Frodo", None],
        "col2": ["the ring", "Morder", "Smeagol"],
        "col3": ["The Lord of the Rings", "Gandalf", ""],
        "col4": [2107, 2108, np.NaN],
        "col5": [1.5, 2.5, np.NaN],
    }
)

# pass as @pytest.mark.parametrize(*df_args, **df_kwargs)
df_args = ["df", [df_simple, df_tricky, df_null_last_col]]
df_kwargs = {"ids": ["df_simple", "df_tricky", "df_null_last_col"]}


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

    @pytest.mark.parametrize(*df_args, **df_kwargs)
    def test_tosql_replace(self, df, sql_creds, database, pyodbc_creds):
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

    @pytest.mark.parametrize(*df_args, **df_kwargs)
    def test_tosql_append(self, df, sql_creds, database, pyodbc_creds):
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

    @pytest.mark.parametrize(*df_args, **df_kwargs)
    def test_tosql_fail(self, df, sql_creds, database, pyodbc_creds):
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

    @pytest.mark.parametrize(*df_args, **df_kwargs)
    def test_tosql_other(self, df, sql_creds, database, pyodbc_creds):
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

    """

    table_name = "lotr_readsql"
    view_name = f"v_{table_name}"

    @pytest.mark.parametrize(*df_args, **df_kwargs)
    def test_readsql_table(self, df, sql_creds, database, pyodbc_creds):
        # create table and insert rows
        df.to_sql(
            name=self.table_name, con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
        )
        # get expected
        expected = read_sql(self.table_name, creds=sql_creds, sql_type="table", schema="dbo")
        # check
        assert_frame_equal(df, expected)

    @pytest.mark.parametrize(*df_args, **df_kwargs)
    def test_readsql_view(self, df, sql_creds, database, pyodbc_creds):
        # create table and insert rows
        df.to_sql(
            name=self.table_name, con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
        )
        # create corresponding view
        sqlcmd(
            creds=sql_creds,
            command="""DROP VIEW IF EXISTS dbo.{v}; 
                GO 
                CREATE VIEW dbo.{v} AS SELECT * FROM dbo.{t};
                GO""".format(
                v=self.view_name, t=self.table_name
            ),
        )

        # get expected
        expected = read_sql(self.view_name, creds=sql_creds, sql_type="view", schema="dbo")
        # check
        assert_frame_equal(df, expected)

    @pytest.mark.parametrize(*df_args, **df_kwargs)
    def test_readsql_query(self, df, sql_creds, database, pyodbc_creds):
        # create table and insert rows
        df.to_sql(
            name=self.table_name, con=pyodbc_creds, if_exists="replace", index=False, schema="dbo"
        )
        # get expected
        expected = read_sql(
            f"SELECT * FROM {self.table_name}", creds=sql_creds, sql_type="query", schema="dbo"
        )
        # check
        assert_frame_equal(df, expected)

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


@pytest.mark.skip(reason="not implemented yet")
class TestSqlCmd:
    pass
