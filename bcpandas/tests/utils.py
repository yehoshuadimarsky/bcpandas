from bcpandas.constants import _DELIMITER_OPTIONS, _QUOTECHAR_OPTIONS
from hypothesis import assume
from hypothesis.extra import pandas as hpd
import hypothesis.strategies as st
import pandas as pd
import pyodbc
import sqlalchemy as sa

# Hypo - typical use cases
#   - DataFrame: at least one row
#   - Text: All text in ASCII 32-127
#   - Integers: between -2**31-1 and 2**31-1
#   - Floats: between -2**31-1 and 2**31-1, without NaN or inf
#   - Dates
#   - Datetimes

MAX_VAL = 2 ** 31 - 1

# Strategies
strat_text = st.text(alphabet=st.characters(min_codepoint=32, max_codepoint=127), min_size=0)
strat_ints = st.integers(min_value=-MAX_VAL, max_value=MAX_VAL)
strat_floats = st.floats(
    min_value=-MAX_VAL, max_value=MAX_VAL, allow_nan=False, allow_infinity=False
)
strat_dates = st.dates()

strat_df_index = hpd.range_indexes(min_size=1)


df_hypo_mixed = hpd.data_frames(
    columns=[
        hpd.column(name="col1_text", elements=strat_text),
        hpd.column(name="col2_ints", elements=strat_ints),
        hpd.column(name="col3_floats", elements=strat_floats),
        hpd.column(name="col4_dates", elements=strat_dates),
    ],
    index=strat_df_index,
)

df_hypo_text = hpd.data_frames(columns=hpd.columns(5, elements=strat_text), index=strat_df_index)
df_hypo_ints = hpd.data_frames(columns=hpd.columns(5, elements=strat_ints), index=strat_df_index)
df_hypo_floats = hpd.data_frames(
    columns=hpd.columns(5, elements=strat_floats), index=strat_df_index
)
df_hypo_dates = hpd.data_frames(columns=hpd.columns(5, elements=strat_dates), index=strat_df_index)


hypo_df = hpd.data_frames(
    columns=[
        hpd.column(name="col1_text", elements=strat_text),
        hpd.column(name="col2_ints", elements=strat_ints),
        hpd.column(name="col3_floats", elements=strat_floats),
        hpd.column(name="col4_dates", elements=strat_dates),
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


def assume_not_all_delims_and_quotechars(df):
    return assume(not_has_all_delims(df) and not_has_all_quotechars(df))


def prep_df_for_comparison(df: pd.DataFrame, index: bool) -> pd.DataFrame:
    """
    Prepares a test dataframe for comparison with its corresponding data that is read from SQL.
    Becuase SQL does some implicit conversions, we need to make the df match that.

    Also uses the index=True/False param to set the df here to the expected value.
    """
    # if index=True, then make the df match the actual in SQL
    if index:
        df = df.reset_index()

    # SQL stores column names that are numbers as text, so we convert the dataframe columns to str
    # so that pandas-sql comparison will not fail
    try:
        df.columns = df.columns.astype(str)
    except TypeError:
        pass

    # Empty string becomes NULL in SQL (None in pandas), marking as ok for now
    if "object" in df.dtypes.values:
        df = df.replace({"": None})
    return df


def execute_sql_statement(sql_alchemy_engine: sa.engine.Engine, statement: str):
    """
    Executes the SQL statement using the provided engine. Assumes uses pyodbc.

    need to use pyodbc instead of sqlalchemy, otherwise get error:
    [SQL Server]CREATE DATABASE statement not allowed within multi-statement transaction.
    """
    conn = pyodbc.connect(sql_alchemy_engine.url.query["odbc_connect"], autocommit=True)
    conn.execute(statement)
    conn.close()
