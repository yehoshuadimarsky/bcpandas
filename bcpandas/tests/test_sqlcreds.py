# -*- coding: utf-8 -*-
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

import urllib
import pytest
import pandas as pd
from bcpandas import SqlCreds
from sqlalchemy import engine, create_engine


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
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,1433;Database=test_database;"
        "UID=test_user;PWD=test_password"
    )


def test_sql_creds_for_windows_auth():
    """
    Tests that the SqlCreds object can be created without Username and Password (Windows Auth)
    """
    creds = SqlCreds(server="test_server", database="test_database", driver_version=99,)
    assert creds.server == "test_server"
    assert creds.database == "test_database"
    assert creds.username == ""
    assert creds.password == ""
    assert creds.port == 1433
    assert creds.with_krb_auth is True
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
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
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,9999;Database=test_database;"
        "UID=test_user;PWD=test_password"
    )


def test_sql_creds_for_windows_auth_non_default_port():
    """
    Tests that the SqlCreds object can be created without Username and Password (Windows Auth)
    """
    creds = SqlCreds(server="test_server", database="test_database", driver_version=99, port=9999,)
    assert creds.port == 9999
    assert creds.with_krb_auth is True
    assert isinstance(creds.engine, engine.Connectable)
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
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
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database;"
        "UID=test_user;PWD=test_password"
    )


def test_sql_creds_for_windows_auth_blank_port():
    """
    Tests that the SqlCreds object can be created
    
    * Without Username and Password (Windows Auth)
    * Use blank port
    """
    creds = SqlCreds(server="test_server", database="test_database", driver_version=99, port=None,)
    assert creds.port is None
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database;Trusted_Connection=yes;"
    )


def test_sql_creds_from_sqlalchemy():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine
    
    * With Username and Password
    * Use default port (1433)
    """
    params = urllib.parse.quote_plus(
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
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        + "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,1433;Database=test_database;"
        + "UID=test_user;PWD=test_password"
    )


def test_sql_creds_from_sqlalchemy_windows_auth():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine
    
    * Without Username and Password
    * Use default port (1433)
    """
    params = urllib.parse.quote_plus(
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
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        + "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,1433;Database=test_database"
    )


def test_sql_creds_from_sqlalchemy_non_default_port():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine

    * With Username and Password
    * Non-Default Port specified (9999)
    """
    params = urllib.parse.quote_plus(
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
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        + "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,9999;Database=test_database;"
        + "UID=test_user;PWD=test_password"
    )


def test_sql_creds_from_sqlalchemy_windows_auth_non_default_port():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine
    
    * Without Username and Password
    * Non-Default Port specifed (9999)
    """
    params = urllib.parse.quote_plus(
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
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        + "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server,9999;Database=test_database"
    )


def test_sql_creds_from_sqlalchemy_blank_port():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine with no Port specified
    """
    params = urllib.parse.quote_plus(
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
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        + "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database;"
        + "UID=test_user;PWD=test_password"
    )


def test_sql_creds_from_sqlalchemy_windows_auth_blank_port():
    """
    Tests that the SqlCreds object can be created from a SqlAlchemy engine - without Username, Password or Port
    """
    params = urllib.parse.quote_plus(
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
    assert str(creds.engine.url) == (
        "mssql+pyodbc:///?odbc_connect="
        + "Driver={ODBC Driver 99 for SQL Server};Server=tcp:test_server;Database=test_database"
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
    params = urllib.parse.quote_plus(conn_str)

    mssql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    # Re-interpret using SqlCreds
    test_engine = SqlCreds.from_engine(mssql_engine).engine

    # Check the SqlCreds version works
    df = pd.read_sql(con=test_engine, sql="SELECT TOP 1 * FROM sys.objects")

    assert df.shape[0] == 1
