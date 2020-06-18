# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima


There are 2 categories of tests we want to do:
    - Test every code path, i.e. every combination of arguments for each function
    - Test with different datasets that have different properties
"""

from os.path import expandvars
from pathlib import Path
import sys
from typing import Optional, no_type_check

from bcpandas import to_sql
from bcpandas.constants import _DELIMITER_OPTIONS, _QUOTECHAR_OPTIONS, BCPandasValueError
from hypothesis import HealthCheck, given, settings
import hypothesis.strategies as st
import numpy as np
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


@pytest.mark.usefixtures("database")
def test_tosql_all_delims(sql_creds):
    df = pd.DataFrame(
        {i: [v, "random", "string", 1, 2.0] for i, v in enumerate(_DELIMITER_OPTIONS)}
    )
    with pytest.raises(BCPandasValueError):
        to_sql(df=df, table_name="tbl_all_delims", creds=sql_creds, if_exists="replace")


@pytest.mark.usefixtures("database")
def test_tosql_all_quotechars(sql_creds):
    df = pd.DataFrame(
        {i: [v, "random", "string", 1, 2.0] for i, v in enumerate(_QUOTECHAR_OPTIONS)}
    )
    with pytest.raises(BCPandasValueError):
        to_sql(df=df, table_name="tbl_all_delims", creds=sql_creds, if_exists="replace")


def _get_bcp_path() -> Optional[str]:
    """
    On Windows, is typically in C:/Program Files/Microsoft SQL Server/Client SDK/ODBC/130/Tools/Binn
    On Linux it depends where you install it, but usually in /opt/mssql-tools/bin
    """
    try:
        if sys.platform == "win32":
            first_part = (
                Path(expandvars("%ProgramFiles%")) / "Microsoft SQL Server" / "Client SDK" / "ODBC"
            )
            version = max(x.parts[-1] for x in first_part.iterdir())
            bcp_path = first_part / version / "Tools" / "Binn"
            return str(bcp_path / "bcp.exe")
        else:
            bcp_path = Path("/opt/mssql-tools/bin")
            return str(bcp_path / "bcp")
    except FileNotFoundError:
        return None


_bcp_path = _get_bcp_path()


@pytest.mark.parametrize(
    "bcp_path",
    [
        pytest.param(
            _bcp_path,
            id=_bcp_path,
            marks=[
                pytest.mark.skipif(
                    _bcp_path is None, reason=f"Didn't find full path to BCP on {sys.platform}"
                )
            ],
        )
    ],
)
@pytest.mark.usefixtures("database")
def test_tosql_full_bcp_path(bcp_path, sql_creds):
    df = pd.DataFrame({"col1": ["a", "b", "c", "d"], "col2": [1.5, 2.5, 3.5, 4.5]})
    tbl_name = "tbl_df_full_bcp_path"
    schema_name = "dbo"
    execute_sql_statement(sql_creds.engine, f"DROP TABLE IF EXISTS {schema_name}.{tbl_name}")
    to_sql(
        df=df,
        table_name=tbl_name,
        creds=sql_creds,
        schema=schema_name,
        if_exists="replace",
        index=False,
        bcp_path=bcp_path,
    )

    # check result
    actual = pd.read_sql_query(sql=f"SELECT * FROM {schema_name}.{tbl_name}", con=sql_creds.engine)
    expected = prep_df_for_comparison(df=df, index=False)
    assert_frame_equal(expected, actual)


@pytest.mark.usefixtures("database")
@no_type_check  # gives wierd errors in list values of dfs in params
@pytest.mark.parametrize(
    "df",
    [
        pytest.param(
            pd.DataFrame(
                {
                    "col1": ["a", "b", "c", "d"],
                    "col2": [1, np.NaN, 3, np.NaN],
                    "col3": [1.5, 2.5, 3.5, 4.5],
                }
            ),
            id="nan",
        ),
        pytest.param(
            pd.DataFrame(
                {
                    "col1": ["a", "b", "c", "d"],
                    "col2": [1, 2, 3, 4],
                    "col3": [1.5, np.NaN, 3.5, np.NaN],
                }
            ),
            id="nan_last_col",
        ),
        pytest.param(
            pd.DataFrame(
                {"col1": [1.5, 2.5, 3.5, 4.5], "col2": [1, 2, 3, 4], "col3": ["a", None, "c", None]}
            ),
            id="None_last_col",
        ),
        pytest.param(
            pd.DataFrame(
                {
                    "col1": [1.5, np.inf, 3.5, 4.5],
                    "col2": [1, 2, np.inf, 4],
                    "col3": ["a", "b", "c", None],
                }
            ),
            id="inf",
            marks=pytest.mark.xfail,
        ),
    ],
)
def test_tosql_nan_null_inf(df, sql_creds):
    tbl_name = "tbl_df_nan_null_last_col"
    schema_name = "dbo"
    execute_sql_statement(sql_creds.engine, f"DROP TABLE IF EXISTS {schema_name}.{tbl_name}")
    to_sql(
        df=df,
        table_name=tbl_name,
        creds=sql_creds,
        schema=schema_name,
        if_exists="replace",
        index=False,
    )

    # check result
    actual = pd.read_sql_query(sql=f"SELECT * FROM {schema_name}.{tbl_name}", con=sql_creds.engine)
    expected = prep_df_for_comparison(df=df, index=False)
    assert_frame_equal(expected, actual)


@pytest.mark.usefixtures("database")
@pytest.mark.parametrize(
    "df",
    [pd.DataFrame({}), pd.DataFrame({"a": []}), pd.DataFrame(index=["a"])],
    ids=["df_empty", "df_with_col", "df_with_idx"],
)
def test_tosql_empty_df(df, sql_creds):
    tbl_name = "tbl_df_empty"
    schema_name = "dbo"
    execute_sql_statement(sql_creds.engine, f"DROP TABLE IF EXISTS {schema_name}.{tbl_name}")
    to_sql(df=df, table_name=tbl_name, creds=sql_creds, schema=schema_name, if_exists="replace")
    # make sure nothing happened in the database
    qry = """
        SELECT * 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{_schema}' 
        AND TABLE_NAME = '{_tbl}'
        """.format(
        _tbl=tbl_name, _schema=schema_name
    )
    res = pd.read_sql_query(sql=qry, con=sql_creds.engine)
    # assert that rows == 0, it has columns even without rows because it is an internal system table
    assert res.shape[0] == 0


@pytest.mark.usefixtures("database")
def test_duplicate_columns(sql_creds):
    table_name = "tosql_column_scenarios_1"
    df = pd.DataFrame(
        {
            "col1": [1.5, 2.5, 3.5, 4.5],
            "col2": [5.5, 6.5, 7.5, 8.5],
            "col3": [9.5, 10.5, 11.5, 12.5],
            "col4": [13.5, 14.5, 15.5, 16.5],
        }
    )

    # set duplicate column
    df = df.rename(columns={"col3": "col2"})

    with pytest.raises(BCPandasValueError):
        to_sql(
            df=df,
            table_name=table_name,
            creds=sql_creds,
            if_exists="replace",
            index=False,
            sql_type="table",
        )


@pytest.mark.skip(reason="Didn't test yet")
def test_non_string_columns():
    # TODO
    pass


@pytest.mark.parametrize("batch_size", [0, 50])
def test_batch_size_param(sql_creds, batch_size):
    """
    Test that batch size can't be:
        - 0
        - larger than the number of rows
    """
    df = pd.DataFrame(
        {
            "col1": [1.5, 2.5, 3.5, 4.5],
            "col2": [5.5, 6.5, 7.5, 8.5],
            "col3": [9.5, 10.5, 11.5, 12.5],
            "col4": [13.5, 14.5, 15.5, 16.5],
        }
    )
    with pytest.raises(BCPandasValueError):
        to_sql(
            df=df,
            table_name="some_table",
            creds=sql_creds,
            if_exists="replace",
            index=False,
            sql_type="table",
            batch_size=batch_size,
        )


@pytest.mark.usefixtures("database")
class _BaseToSql:
    sql_type = "table"
    schema_name = "dbo"

    # per https://github.com/pytest-dev/pytest/issues/2618#issuecomment-318584202
    fixture_names = ("sql_creds", "pyodbc_creds")

    @pytest.fixture(autouse=True, scope="function")
    def auto_injector_fixture(self, request):
        for name in self.fixture_names:
            setattr(self, name, request.getfixturevalue(name))


class TestToSqlColumnScenarios(_BaseToSql):
    """
    Various tests for scenarios where the dataframe columns don't map exactly to the database columns,
    whether by different order, missing columns, etc.
    """

    table_name = "tosql_column_scenarios"
    df = pd.DataFrame(
        {
            "col1": [1.5, 2.5, 3.5, 4.5],
            "col2": [5.5, 6.5, 7.5, 8.5],
            "col3": [9.5, 10.5, 11.5, 12.5],
            "col4": [13.5, 14.5, 15.5, 16.5],
        }
    )

    def _create_tbl_with_data(self):
        # first create a new table and put some data into it
        execute_sql_statement(
            self.sql_creds.engine, f"DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}"
        )
        to_sql(
            df=self.df,
            table_name=self.table_name,
            creds=self.sql_creds,
            if_exists="replace",
            index=False,
            sql_type=self.sql_type,
        )

    def _check_result(self, df_modified):
        # check result
        actual = pd.read_sql_query(
            sql=f"SELECT * FROM {self.schema_name}.{self.table_name}", con=self.sql_creds.engine
        )
        expected = prep_df_for_comparison(
            df=pd.concat([self.df, df_modified], axis=0, ignore_index=True, sort=False), index=False
        )
        assert_frame_equal(expected, actual)

    def test_skip_cols(self):
        """
        see https://docs.microsoft.com/en-us/sql/relational-databases/import-export/use-a-format-file-to-skip-a-table-column-sql-server
        """
        # first create a new table and put some data into it
        self._create_tbl_with_data()

        # then insert (append) data into only some of the columns
        df_changed = self.df.iloc[:, 1:]
        to_sql(
            df=df_changed,
            table_name=self.table_name,
            creds=self.sql_creds,
            if_exists="append",
            index=False,
            sql_type=self.sql_type,
        )

        # check result
        self._check_result(df_changed)

    def test_cols_out_of_order(self):
        """
        see https://docs.microsoft.com/en-us/sql/relational-databases/import-export/use-a-format-file-to-map-table-columns-to-data-file-fields-sql-server
        """
        # first create a new table and put some data into it
        self._create_tbl_with_data()

        # then insert (append) data with columns out of order
        df_changed = self.df.iloc[:, ::-1]  # columns reversed
        to_sql(
            df=df_changed,
            table_name=self.table_name,
            creds=self.sql_creds,
            if_exists="append",
            index=False,
            sql_type=self.sql_type,
        )

        # check result
        self._check_result(df_changed)

    def test_cols_skip_and_out_of_order(self):
        # first create a new table and put some data into it
        self._create_tbl_with_data()

        # then insert data into only some of the columns, and out of order
        df_changed = self.df.loc[:, ["col4", "col1", "col2"]]
        to_sql(
            df=df_changed,
            table_name=self.table_name,
            creds=self.sql_creds,
            if_exists="append",
            index=False,
            sql_type=self.sql_type,
        )

        # check result
        self._check_result(df_changed)

    def test_extra_col(self):
        # first create a new table and put some data into it
        self._create_tbl_with_data()

        # then insert data into only some of the columns, and out of order
        df_changed = self.df.copy()
        df_changed.loc[:, "col_N"] = [17.5, 18.5, 19.5, 20.5]

        with pytest.raises(BCPandasValueError):
            to_sql(
                df=df_changed,
                table_name=self.table_name,
                creds=self.sql_creds,
                if_exists="append",
                index=False,
                sql_type=self.sql_type,
            )


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
