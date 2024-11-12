import sys
from collections import namedtuple
from pathlib import Path
import tempfile
from unittest import mock

import pandas as pd
import pytest

from bcpandas import SqlCreds, utils
from bcpandas.constants import IN, BCPandasException


@pytest.fixture(name="run_cmd")
def fixture_run_cmd_capture(monkeypatch):
    run_cmd = mock.MagicMock(return_value=(0, []))
    monkeypatch.setattr(utils, "run_cmd", run_cmd)
    return run_cmd


def test_bcpandas_creates_command_without_port_if_default(run_cmd):
    Creds = namedtuple(
        "Creds", "server port database with_krb_auth username password odbc_kwargs entra_id_token"
    )
    creds = Creds(
        server="localhost",
        port=1433,
        database="DB",
        with_krb_auth=False,
        username="me",
        password="secret",
        odbc_kwargs=None,
        entra_id_token=None,
    )
    utils.bcp("table", "in", "", creds, True)
    assert run_cmd.call_args == mock.call(
        [
            "bcp",
            "dbo.table",
            "in",
            "",
            "-S",
            "localhost",
            "-d",
            "DB",
            "-q",
            "-U",
            "me",
            "-P",
            "secret",
        ],
        print_output=True,
    )


def test_bcpandas_creates_command_with_port_if_not_default(run_cmd):
    Creds = namedtuple(
        "Creds", "server port database with_krb_auth username password odbc_kwargs entra_id_token"
    )
    creds = Creds(
        server="localhost",
        port=1234,
        database="DB",
        with_krb_auth=False,
        username="me",
        password="secret",
        odbc_kwargs=None,
        entra_id_token=None,
    )
    utils.bcp("table", "in", "", creds, True)
    assert run_cmd.call_args == mock.call(
        [
            "bcp",
            "dbo.table",
            "in",
            "",
            "-S",
            "localhost,1234",
            "-d",
            "DB",
            "-q",
            "-U",
            "me",
            "-P",
            "secret",
        ],
        print_output=True,
    )


def test_bcpandas_creates_command_with_encrypt_no(run_cmd):
    Creds = namedtuple(
        "Creds", "server port database with_krb_auth username password odbc_kwargs entra_id_token"
    )
    creds = Creds(
        server="localhost",
        port=1433,
        database="DB",
        with_krb_auth=False,
        username="me",
        password="secret",
        odbc_kwargs=dict(encrypt="no"),
        entra_id_token=None,
    )
    utils.bcp("table", "in", "", creds, True)
    assert run_cmd.call_args == mock.call(
        [
            "bcp",
            "dbo.table",
            "in",
            "",
            "-S",
            "localhost",
            "-d",
            "DB",
            "-q",
            "-U",
            "me",
            "-P",
            "secret",
            "-Yo",
        ],
        print_output=True,
    )


def test_bcpandas_creates_command_with_encrypt_yes(run_cmd):
    Creds = namedtuple(
        "Creds", "server port database with_krb_auth username password odbc_kwargs entra_id_token"
    )
    creds = Creds(
        server="localhost",
        port=1433,
        database="DB",
        with_krb_auth=False,
        username="me",
        password="secret",
        odbc_kwargs=dict(Encrypt="1"),
        entra_id_token=None,
    )
    utils.bcp("table", "in", "", creds, True)
    assert run_cmd.call_args == mock.call(
        [
            "bcp",
            "dbo.table",
            "in",
            "",
            "-S",
            "localhost",
            "-d",
            "DB",
            "-q",
            "-U",
            "me",
            "-P",
            "secret",
        ]
        + (["-Ym"] if sys.platform != "win32" else []),
        print_output=True,
    )


def test_bcpandas_creates_command_with_entra_id_token(run_cmd):
    Creds = namedtuple(
        "Creds", "server port database with_krb_auth username password odbc_kwargs entra_id_token"
    )
    creds = Creds(
        server="localhost",
        port=1433,
        database="DB",
        with_krb_auth=False,
        username=None,
        password=None,
        odbc_kwargs=dict(Encrypt="1"),
        entra_id_token="secret_token",
    )
    utils.bcp("table", "in", "", creds, True)
    assert run_cmd.call_args == mock.call(
        [
            "bcp",
            "dbo.table",
            "in",
            "",
            "-S",
            "localhost",
            "-d",
            "DB",
            "-q",
            "-G",
            "-P",
            "secret_token",
        ]
        + (["-Ym"] if sys.platform != "win32" else []),
        print_output=True,
    )


@pytest.mark.usefixtures("database")
def test_bcp_login_failure(sql_creds: SqlCreds):
    wrong_sql_creds = SqlCreds(
        server=sql_creds.server,
        database=sql_creds.database,
        username=sql_creds.username,
        password="mywrongpassword",
    )
    df = pd.DataFrame([{"col1": "value"}])
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "data.csv"
        df.to_csv(csv_path)
        try:
            utils.bcp(
                sql_item="tbl_login_failure",
                direction=IN,
                flat_file=csv_path,
                creds=wrong_sql_creds,
                print_output=False,
            )
            pytest.fail("utils.bcp is not expected to succeed")
        except BCPandasException as e:
            assert any("Login failed" in message for message in e.details)
            assert not any("not a real error" in message for message in e.details)
