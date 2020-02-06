from contextlib import contextmanager
import json
import platform
from subprocess import PIPE, run
import sys
from typing import List, Union

from bcpandas import SqlCreds, to_sql
from bcpandas.tests import conftest as cf
from codetiming import Timer
import numpy as np
import pandas as pd

mssql_image = "mcr.microsoft.com/mssql/server:2017-latest"
_IS_WIN32 = sys.platform == "win32"
with_shell = False
if not _IS_WIN32:
    with_shell = True


def _parse_cmd(cmd: List[str]) -> Union[List[str], str]:
    if _IS_WIN32:
        return cmd
    else:
        return " ".join(cmd)


@contextmanager
def capture_stdout():
    # per https://stackoverflow.com/a/1218951/6067848
    from io import StringIO

    try:
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()
        yield mystdout
    finally:
        sys.stdout = old_stdout


def gather_env_info():
    # TODO get RAM and hard drive info
    config = {
        "os_hardware": {},
        "python": {},
        "python_libs": {},
        "mssql": {},
        "mssql_tools": {},
        "docker": {},
    }

    # OS and hardware info
    config["os_hardware"] = {
        i: getattr(platform.uname(), i)
        for i in ["system", "release", "version", "machine", "processor"]
    }
    config["os_hardware"]["python_compiler"] = platform.python_compiler()

    # Python
    config["python"] = {k: v for k, v in sys.implementation.__dict__.items()}

    # Python libraries

    with capture_stdout() as mystdout:
        pd.show_versions(as_json=True)
        pdv = mystdout.getvalue().replace("'", '"').replace("None", "null")
        pdv_j = json.loads(pdv)
    config["python_libs"]["pandas_versions"] = pdv_j

    # MSSQL
    config["mssql"] = {"docker-image": mssql_image, "MSSQL_PID": "Express"}

    # Sql Tools
    cmd_bcp = ["bcp", "-v"]
    res = run(_parse_cmd(cmd_bcp), stdout=PIPE, stderr=PIPE, shell=with_shell)
    if res.returncode == 0:
        config["mssql_tools"] = {"bcp-version": res.stdout.decode().strip()}

    # Docker
    cmd_docker = ["docker", "version"]
    res = run(_parse_cmd(cmd_docker), stdout=PIPE, stderr=PIPE, shell=with_shell)
    if res.returncode == 0:
        config["docker"] = {"docker-version-output": res.stdout.decode()}

    return config


def setup():
    # create docker container
    gen_docker_db = cf.docker_db.__pytest_wrapped__.obj()
    next(gen_docker_db)

    # create database
    gen_database = cf.database.__pytest_wrapped__.obj("dummy param")
    next(gen_database)

    # create creds
    creds = cf.sql_creds.__pytest_wrapped__.obj()

    return gen_docker_db, creds


def teardown(gen_docker_db):
    try:
        next(gen_docker_db)
    except StopIteration:
        pass


def run_benchmark(df: pd.DataFrame, creds: SqlCreds):
    tbl_name = "tbl_benchmark"

    # pandas
    print("starting pandas")
    t1 = Timer(name="pandas")
    t1.start()
    df.to_sql(name=tbl_name, con=creds.engine, if_exists="replace")
    pd_time = t1.stop()

    # bcp
    print("starting bcp")
    t2 = Timer(name="bcp")
    t2.start()
    to_sql(df=df, table_name=tbl_name, creds=creds, if_exists="replace")
    bcp_time = t2.stop()

    return pd_time, bcp_time


def main():
    """

    """
    # run benchmarks
    docker_generator, creds = setup()
    num_cols = 6
    results = []
    for n in np.linspace(500, 50000, num=20):
        num_rows = int(n)
        df = pd.DataFrame(data=np.ndarray(shape=(num_rows, num_cols), dtype=int))
        pd_time, bcp_time = run_benchmark(df=df, creds=creds)
        results.append({"num_rows": num_rows, "pandas_time": pd_time, "bcpandas_time": bcp_time})
    teardown(docker_generator)

    # file names
    data_file = "benchmark_data.json"
    plot_file = "benchmark.png"
    env_file = "benchmark_environment.json"

    # process results
    frame = pd.DataFrame(results).set_index("num_rows")
    frame.to_json(data_file)
    plot = frame.plot(
        kind="line",
        title=f"ToSql Comparison - Integers, {num_cols} columns",
        linestyle="--",
        marker="o",
    )
    plot.set_xlabel("number of rows")
    plot.set_ylabel("time (in seconds)")
    plot.get_figure().savefig(plot_file)
    env_info = gather_env_info()
    with open(env_file, "wt") as file:
        json.dump(env_info, file, indent=2)


if __name__ == "__main__":
    main()
