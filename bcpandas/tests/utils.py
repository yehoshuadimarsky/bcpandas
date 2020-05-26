import urllib

from bcpandas.constants import _DELIMITER_OPTIONS, _QUOTECHAR_OPTIONS
import docker
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
#   - (some) booleans

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
        hpd.column(name="col4_bools", elements=st.booleans()),
    ],
    index=strat_df_index,
)

df_hypo_text = hpd.data_frames(columns=hpd.columns(5, elements=strat_text), index=strat_df_index)
df_hypo_ints = hpd.data_frames(columns=hpd.columns(5, elements=strat_ints), index=strat_df_index)
df_hypo_floats = hpd.data_frames(
    columns=hpd.columns(5, elements=strat_floats), index=strat_df_index
)
df_hypo_dates = hpd.data_frames(columns=hpd.columns(5, elements=strat_dates), index=strat_df_index)


def not_has_all_delims(df: pd.DataFrame) -> bool:
    return not all(
        df.applymap(lambda x: delim in x if isinstance(x, str) else False).any().any()
        for delim in _DELIMITER_OPTIONS
    )


def not_has_all_quotechars(df: pd.DataFrame) -> bool:
    return not all(
        df.applymap(lambda x: qc in x if isinstance(x, str) else False).any().any()
        for qc in _QUOTECHAR_OPTIONS
    )


def assume_not_all_delims_and_quotechars(df: pd.DataFrame) -> bool:
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


class DockerDB:
    """
    Class to create and run a SQL Server database using a Docker container. All the docker stuff is taken care of 
    by Python under the hood.

    Each instance of this class can only support a single specific combination of settings, such as container name
    and SQL password. To create more than one, create more instances of the class.
    """

    def __init__(
        self,
        container_name: str,
        sa_sql_password: str,
        mssql_image: str = "mcr.microsoft.com/mssql/server:2017-latest",
        port_host: int = 1433,
        port_container: int = 1433,
        accept_eula: bool = True,
        mssql_pid: str = "Express",
    ):
        self.client = docker.from_env()
        self.container_name = container_name
        self.sa_sql_password = sa_sql_password
        self.mssql_image = mssql_image
        self.port_host = port_host
        self.port_container = port_container
        self.accept_eula = accept_eula
        self.mssql_pid = mssql_pid

    def start(self):
        if not self.accept_eula:
            raise ValueError("Must accept Microsft's End User License Agreement")
        env = {
            "ACCEPT_EULA": "Y",
            "SA_PASSWORD": self.sa_sql_password,
        }
        if self.mssql_image.startswith("mcr.microsoft.com/mssql/server"):
            # means it's linux
            env["MSSQL_PID"] = self.mssql_pid

        self.container = self.client.containers.run(
            image=self.mssql_image,
            name=self.container_name,
            detach=True,  # '-d' flag
            environment=env,
            ports={self.port_container: self.port_host},
        )

    def stop(self):
        self.container.stop()

    def remove(self):
        self.container.remove()

    def create_engine(self, db_name="master") -> sa.engine.Engine:
        """Creates SQLAlchemy pyodbc engine for connecting to specified database (default master) as SA user"""
        db_url = (
            "Driver={ODBC Driver 17 for SQL Server};"
            + f"Server={self.address};Database={db_name};UID=sa;PWD={self.sa_sql_password};"
        )
        return sa.engine.create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(db_url)}"
        )

    def create_database(self, name):
        """Creates a SQL database on the SQL server"""
        execute_sql_statement(self.create_engine("master"), f"CREATE DATABASE {name}")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        return

    @property
    def address(self):
        return f"127.0.0.1,{self.port_container}"
