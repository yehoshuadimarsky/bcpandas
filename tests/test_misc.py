import pytest

from bcpandas.utils import run_cmd


@pytest.mark.parametrize(
    "cmd,use_print,expected_stdout,expected_stderr",
    [
        (["echo", "hello world from 1"], True, "hello world from 1\n", ""),
        pytest.param(
            ["python", "-c", "\"import sys; print('world2', file=sys.stderr)\""],
            True,
            "",
            "world2\n",
            marks=pytest.mark.skip(reason="Can't get it to write to STDERR for some reason"),
        ),
        (["echo", "hello world from 3"], False, "", ""),
        pytest.param(
            ["python", "-c", "\"import sys; print('world2', file=sys.stderr)\""],
            False,
            "",
            "",
            marks=pytest.mark.skip(reason="Can't get it to write to STDERR for some reason"),
        ),
    ],
)
# https://docs.pytest.org/en/6.2.x/capture.html
def test_run_cmd_print_output(capsys, cmd, use_print, expected_stdout, expected_stderr):
    run_cmd(cmd, print_output=use_print)
    captured = capsys.readouterr()
    assert captured.out == expected_stdout
    assert captured.err == expected_stderr
