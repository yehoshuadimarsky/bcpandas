from bcpandas import read_sql
from bcpandas.constants import BCPandasValueError, read_data_settings
from hypothesis import assume, given, settings
import pandas as pd
from pandas.testing import assert_frame_equal
import pyodbc
import pytest

from .utils import hypo_df, not_has_all_delims, not_has_all_quotechars


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
