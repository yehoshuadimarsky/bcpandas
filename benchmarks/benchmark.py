from contextlib import contextmanager  # noqa: E999
import json
from math import floor
import platform
from subprocess import PIPE, run
import sys
import time
from typing import Dict, List, Union

from bcpandas import SqlCreds, to_sql
from bcpandas.tests.utils import DockerDB
import click
from codetiming import Timer
import numpy as np
import pandas as pd
from read_sql.read_sql import read_sql

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
        config["mssql_tools"] = {"bcp-version": res.stdout.decode().strip().split("\r\n")}

    # Docker
    cmd_docker = ["docker", "version", "--format", "'{{json .}}'"]
    res = run(_parse_cmd(cmd_docker), stdout=PIPE, stderr=PIPE, shell=with_shell)
    if res.returncode == 0:
        docker_out = res.stdout.decode().strip()[1:-1]  # strip outer single quotes
        config["docker"] = {"docker-version-output": json.loads(docker_out)}

    return config


def setup(docker_db):
    # start docker container
    docker_db.start()
    time.sleep(20)  # wait for container to start up

    # create database
    docker_db.create_database("benchmark_db")

    # create creds
    creds = SqlCreds.from_engine(docker_db.create_engine(db_name="benchmark_db"))

    return creds


def teardown(docker_db):
    docker_db.stop()
    docker_db.remove()


def _run_single_func(title, func, **kwargs):
    print(f"starting {title}")
    t = Timer(name=title)
    t.start()
    func(**kwargs)
    elapsed = t.stop()
    print(f"finished {title}")
    return elapsed


def run_benchmark_tosql(df: pd.DataFrame, creds: SqlCreds) -> Dict[str, float]:

    # using multi-insert in MS SQL is limited by hard limit of 2100 params
    # in SQL SPs. Using 2000 to be safe.
    # https://stackoverflow.com/a/56583204/6067848
    chunk_size = floor(2000 / (len(df.columns) + 1))  # +1 in case index=True

    funcs = [
        dict(
            title=f"pandas_multiinsert_{chunk_size}",
            func=df.to_sql,
            name="tbl_pandas_1",
            con=creds.engine,
            if_exists="replace",
            method="multi",
            chunksize=chunk_size,
        ),
        dict(
            title=f"bcpandas_batchsize_{chunk_size}",
            func=to_sql,
            df=df,
            table_name="tbl_bcpandas_1",
            creds=creds,
            if_exists="replace",
            batch_size=chunk_size,
        ),
        dict(
            title="bcpandas_batchsize_10000",
            func=to_sql,
            df=df,
            table_name="tbl_bcpandas_2",
            creds=creds,
            if_exists="replace",
            batch_size=10000,
        ),
    ]

    return {i["title"]: _run_single_func(**i) for i in funcs}


def run_benchmark_readsql(df: pd.DataFrame, creds: SqlCreds) -> Dict[str, float]:
    chunk_size = floor(2000 / (len(df.columns) + 1))  # +1 in case index=True

    # first create table and insert rows
    tbl_name = "sql_tbl_read_sql"
    to_sql(
        df,
        table_name=tbl_name,
        creds=creds,
        sql_type="table",
        schema="dbo",
        index=False,
        if_exists="replace",
        batch_size=10_000,
    )

    funcs = [
        dict(
            title=f"pandas_readsql_{chunk_size}",
            func=pd.read_sql_table,
            schema="dbo",
            table_name=tbl_name,
            con=creds.engine,
            chunksize=chunk_size,
        ),
        dict(
            title=f"bcpandas_batchsize_{chunk_size}_check_delim",
            func=read_sql,
            table_name=tbl_name,
            creds=creds,
            sql_type="table",
            schema="dbo",
            batch_size=chunk_size,
            check_delim=True,
        ),
        dict(
            title=f"bcpandas_batchsize_{chunk_size}_no_check_delim",
            func=read_sql,
            table_name=tbl_name,
            creds=creds,
            sql_type="table",
            schema="dbo",
            batch_size=chunk_size,
            check_delim=False,
        ),
    ]

    return {i["title"]: _run_single_func(**i) for i in funcs}


def save_and_plot(func, results, num_cols):
    # file names
    data_file = f"{func}_benchmark_data.json"
    plot_file = f"{func}_benchmark.png"
    env_file = f"{func}_benchmark_environment.json"

    # process results
    frame = pd.DataFrame(results).set_index("num_rows")
    frame.to_json(data_file)
    read_or_write = func.split("sql")[0]
    plot = frame.plot(
        kind="line",
        title=f"{read_or_write}_sql Comparison - Integers, {num_cols} columns",
        linestyle="--",
        marker="o",
    )
    plot.set_xlabel("number of rows")
    plot.set_ylabel("time (in seconds)")
    # https://stackoverflow.com/a/44444489/6067848
    plot.set_xticklabels([f"{x:,.0f}" for x in plot.get_xticks()])
    plot.get_figure().savefig(plot_file)
    env_info = gather_env_info()
    with open(env_file, "wt") as file:
        json.dump(env_info, file, indent=2)


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "-f",
    "--func",
    type=click.Choice(["tosql", "readsql"], case_sensitive=False),
    required=True,
    help="The Bcpandas function to benchmark",
)
@click.option(
    "--num-cols", type=int, default=6, show_default=True, help="Number of columns in the DataFrames"
)
@click.option(
    "--min-rows",
    type=click.IntRange(0,),
    default=50_000,
    show_default=True,
    help="Min rows in the DataFrames",
)
@click.option(
    "--max-rows", type=int, default=1_000_000, show_default=True, help="Max rows in the DataFrames"
)
@click.option(
    "--num-examples", type=int, default=10, show_default=True, help="How many Dataframes to run"
)
def main(func, num_cols, min_rows, max_rows, num_examples):
    """
    Will generate `num-examples` of DataFrames using numpy.linspace, going from `min-rows` rows to
    `max-rows` rows.
    """
    bmark_name = f"Benchmark run: func={func}, num_cols={num_cols}, min_rows={min_rows}, max_rows={max_rows}, num_examples={num_examples}"

    print(f"Starting {bmark_name}")
    timer = Timer(name=bmark_name)
    timer.start()
    docker_db = DockerDB("bcpandas-benchmarks", "MyBigSQLPasswordAlso!!!")
    try:
        # run benchmarks
        creds = setup(docker_db)
        results = []
        for n in np.linspace(min_rows, max_rows, num=num_examples):
            num_rows = int(n)
            df = pd.DataFrame(
                data=np.ndarray(shape=(num_rows, num_cols), dtype=int),
                columns=[f"col-{x}" for x in range(num_cols)],
            )
            if func == "readsql":
                _results = run_benchmark_readsql(df=df, creds=creds)
            elif func == "tosql":
                _results = run_benchmark_tosql(df=df, creds=creds)
            results.append({"num_rows": num_rows, **_results})
    finally:
        teardown(docker_db)

    save_and_plot(func=func, results=results, num_cols=num_cols)


if __name__ == "__main__":
    cli()
