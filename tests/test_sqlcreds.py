"""
Created on Sat June 27 2020

@author: sstride

Tests included in this file:
    - Test instantiation of SqlCreds with username/password or without (Windows auth)
    - Test instantiation of SqlCreds with/without username/password with non-default port (9999)
    - Test instantiation of SqlCreds with/without username/password with no port specified
    - Test creation of SqlCreds from SqlAlchemy with/without username/password
    - Test creation of SqlCreds from SqlAlchemy with/without username/password with non-default port (9999)
    - Test creation of SqlCreds from SqlAlchemy with/without username/password with no port specified
    - Test connection to docker instance with username/password
    - Test connection to docker instance with username/password interpretted from SqlAlchemy Engine

Not included (yet):
    - *Actually connecting* to SQL Server with Windows Auth

# TODO creating SqlCreds from SqlAlchemy engine case insensitivity
"""
from functools import lru_cache
from urllib.parse import quote_plus

from packaging.version import Version, parse
import pandas as pd
import pytest
from sqlalchemy import create_engine, engine

from bcpandas import SqlCreds


@lru_cache(maxsize=256)
def _get_sqlalchemy_version() -> Version:
    import sqlalchemy as sa

    version = parse(sa.__version__)
    return version


def _quote_engine_url(conn_str: str) -> str:
    prefix = "mssql+pyodbc:///?odbc_connect="
    sa_version = _get_sqlalchemy_version()
    # sqlalchemy >=1.3.18 needs quoting
    # https://docs.sqlalchemy.org/en/14/changelog/changelog_13.html#change-a4bcb1e1c47b780b849c28fea285c81c
    if sa_version >= Version("1.3.18"):
        return prefix + quote_plus(conn_str)
    else:
        return prefix + conn_str


def test_sql_creds_for_username_password():
    """
    Tests that the SqlCreds object can be created with Username and Password (SQL Auth)
    """
    creds = SqlCreds(
        server="test_server",
        database="test_database",
        username="test_user",
        password="test_password",
        driver_version=99,
    )
    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == "test_user"
    assert creds.password == "test_password"
    assert creds.port == 1433
    assert creds.with_krb_auth is False
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,1433;Database=test_database;"
        "UID=test_user;PWD=test_password"
    )


def test_sql_creds_for_username_password_version_not_specified():
    """
    Tests that the SqlCreds object can be created with Username and Password (SQL Auth) without also specifying driver_version
    """
    creds = SqlCreds(
        server="test_server",
        database="test_database",
        username="test_user",
        password="test_password",
    )

    url_split = str(creds.engine.url).split(";")
    url_driver = url_split[0]
    url_driver_no_version = "".join([letter for letter in url_driver if not letter.isnumeric()])
    new_url = url_driver_no_version + ";".join(url_split[1:])

    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == "test_user"
    assert creds.password == "test_password"
    assert creds.port == 1433
    assert creds.with_krb_auth is False
    assert isinstance(creds.engine, engine.Connectable)
    assert (
        new_url
        == "mssql+pyodbc:///?odbc_connect=Driver%D%BODBC+Driver++for+SQL+Server%D%BServer%Dtcp%Atest_server%C%BDatabase%Dtest_database%BUID%Dtest_user%BPWD%Dtest_password"
    )


def test_sql_creds_for_windows_auth():
    """
    Tests that the SqlCreds object can be created without Username and Password (Windows Auth)
    """
    creds = SqlCreds(
        server="test_server",
        database="test_database",
        driver_version=99,
    )
    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == ""
    assert creds.password == ""
    assert creds.port == 1433
    assert creds.with_krb_auth is True
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,1433;Database=test_database;Trusted_Connection=yes;"
    )
    # n.b. this is automatically appending tcp: and port 1433


def test_sql_creds_for_username_password_non_default_port():
    """
    Tests that the SqlCreds object can be created with Username and Password (SQL Auth)
    """
    creds = SqlCreds(
        server="test_server",
        database="test_database",
        username="test_user",
        password="test_password",
        driver_version=99,
        port=9999,
    )
    assert creds.port == 9999
    assert creds.with_krb_auth is False
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,9999;Database=test_database;"
        "UID=test_user;PWD=test_password"
    )


def test_sql_creds_for_windows_auth_non_default_port():
    """
    Tests that the SqlCreds object can be created without Username and Password (Windows Auth)
    """
    creds = SqlCreds(
        server="test_server",
        database="test_database",
        driver_version=99,
        port=9999,
    )
    assert creds.port == 9999
    assert creds.with_krb_auth is True
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,9999;Database=test_database;Trusted_Connection=yes;"
    )


def test_sql_creds_for_username_password_blank_port():
    """
    Tests that the SqlCreds object can be created with Username and Password (SQL Auth) and blank Port

    * With Username and Password
    * Use blank port
    """
    creds = SqlCreds(
        server="test_server",
        database="test_database",
        username="test_user",
        password="test_password",
        driver_version=99,
        port=None,
    )
    assert creds.port is None
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database;"
        "UID=test_user;PWD=test_password"
    )


def test_sql_creds_for_windows_auth_blank_port():
    """
    Tests that the SqlCreds object can be created

    * Without Username and Password (Windows Auth)
    * Use blank port
    """
    creds = SqlCreds(
        server="test_server",
        database="test_database",
        driver_version=99,
        port=None,
    )
    assert creds.port is None
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database;Trusted_Connection=yes;"
    )


def test_sql_creds_from_sqlalchemy():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine

    * With Username and Password
    * Use default port (1433)
    """
    params = quote_plus(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,1433;Database=test_database;"
        "UID=test_user;PWD=test_password"
    )
    mssql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    creds = SqlCreds.from_engine(mssql_engine)
    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == "test_user"
    assert creds.password == "test_password"
    assert creds.port == 1433
    assert creds.with_krb_auth is False
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,1433;Database=test_database;"
        + "UID=test_user;PWD=test_password"
    )


def test_sql_creds_from_sqlalchemy_windows_auth():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine

    * Without Username and Password
    * Use default port (1433)
    """
    params = quote_plus(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,1433;Database=test_database"
    )
    mssql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    creds = SqlCreds.from_engine(mssql_engine)
    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == ""
    assert creds.password == ""
    assert creds.port == 1433
    assert creds.with_krb_auth is True
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,1433;Database=test_database"
    )


def test_sql_creds_from_sqlalchemy_non_default_port():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine

    * With Username and Password
    * Non-Default Port specified (9999)
    """
    params = quote_plus(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,9999;Database=test_database;"
        "UID=test_user;PWD=test_password"
    )
    mssql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    creds = SqlCreds.from_engine(mssql_engine)
    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == "test_user"
    assert creds.password == "test_password"
    assert creds.port == 9999
    assert creds.with_krb_auth is False
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,9999;Database=test_database;"
        + "UID=test_user;PWD=test_password"
    )


def test_sql_creds_from_sqlalchemy_windows_auth_non_default_port():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine

    * Without Username and Password
    * Non-Default Port specifed (9999)
    """
    params = quote_plus(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,9999;Database=test_database"
    )
    mssql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    creds = SqlCreds.from_engine(mssql_engine)
    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == ""
    assert creds.password == ""
    assert creds.port == 9999
    assert creds.with_krb_auth is True
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,9999;Database=test_database"
    )


def test_sql_creds_from_sqlalchemy_blank_port():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine with no Port specified
    """
    params = quote_plus(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database;"
        "UID=test_user;PWD=test_password"
    )
    mssql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    creds = SqlCreds.from_engine(mssql_engine)
    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == "test_user"
    assert creds.password == "test_password"
    assert creds.port is None
    assert creds.with_krb_auth is False
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database;"
        + "UID=test_user;PWD=test_password"
    )


def test_sql_creds_from_sqlalchemy_windows_auth_blank_port():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine - without Username, Password or Port
    """
    params = quote_plus(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database"
    )
    mssql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    creds = SqlCreds.from_engine(mssql_engine)
    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == ""
    assert creds.password == ""
    assert creds.port is None
    assert creds.with_krb_auth is True
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == _quote_engine_url(
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database"
    )


@pytest.mark.usefixtures("database")
def test_sqlcreds_connection(sql_creds):
    """
    Simple test to ensure that the generated creds can connect to the database

    The sql_creds fixture necessarily uses username and password (no Windows auth)
    """

    df = pd.read_sql(con=sql_creds.engine, sql="SELECT TOP 1 * FROM sys.objects")

    assert df.shape[0] == 1


@pytest.mark.usefixtures("database")
def test_sqlcreds_connection_from_sqlalchemy(sql_creds):
    """
    Simple test to ensure that the generated creds can connect to the database by
    interpretting the connection from a SQLAlchemy engine

    The sql_creds fixture necessarily uses username and password (no Windows auth)
    """

    # Create an actual SQL Alchemy connection
    # n.b. Keys are case sensitive
    conn_str = (
        f"DRIVER={sql_creds.driver};"
        f"Server={sql_creds.server};"
        f"Database={sql_creds.database};"
        f"UID={sql_creds.username};"
        f"PWD={sql_creds.password};"
    )
    params = quote_plus(conn_str)

    mssql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    # Re-interpret using SqlCreds
    test_engine = SqlCreds.from_engine(mssql_engine).engine

    # Check the SqlCreds version works
    df = pd.read_sql(con=test_engine, sql="SELECT TOP 1 * FROM sys.objects")

    assert df.shape[0] == 1
