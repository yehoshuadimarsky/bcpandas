from collections import namedtuple
from unittest import mock

import pytest

from bcpandas import utils


@pytest.fixture(name="run_cmd")
def fixture_run_cmd_capture(monkeypatch):
    run_cmd = mock.MagicMock(return_value=0)
    monkeypatch.setattr(utils, "run_cmd", run_cmd)
    return run_cmd


def test_bcpandas_creates_command_without_port_if_default(run_cmd):
    Creds = namedtuple("Creds", "server port database with_krb_auth username password")
    creds = Creds(
        server="localhost",
        port=1433,
        database="DB",
        with_krb_auth=False,
        username="me",
        password="secret",
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
    Creds = namedtuple("Creds", "server port database with_krb_auth username password")
    creds = Creds(
        server="localhost",
        port=1234,
        database="DB",
        with_krb_auth=False,
        username="me",
        password="secret",
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
