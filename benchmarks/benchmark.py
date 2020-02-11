from contextlib import contextmanager
import json
import platform
from subprocess import PIPE, run
import sys
from typing import Dict, List, Union

from bcpandas import SqlCreds, to_sql
from bcpandas.tests import conftest as cf
import click
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
        config["mssql_tools"] = {"bcp-version": res.stdout.decode().strip().split("\r\n")}

    # Docker
    cmd_docker = ["docker", "version", "--format", "'{{json .}}'"]
    res = run(_parse_cmd(cmd_docker), stdout=PIPE, stderr=PIPE, shell=with_shell)
    if res.returncode == 0:
        docker_out = res.stdout.decode().strip()[1:-1]  # strip outer single quotes
        config["docker"] = {"docker-version-output": json.loads(docker_out)}

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


def _run_single_func(title, func, **kwargs):
    print(f"starting {title}...")
    t = Timer(name=title)
    t.start()
    func(**kwargs)
    elapsed = t.stop()
    return elapsed


def run_benchmark(df: pd.DataFrame, creds: SqlCreds) -> Dict[str, float]:

    # using multi-insert in MS SQL is limited by hard limit of 2100 params
    # in SQL SPs. Using 2000 to be safe.
    # https://stackoverflow.com/a/56583204/6067848
    from math import floor

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
    "--num-cols", type=int, required=True, default=6, help="Number of columns in the DataFrames"
)
@click.option(
    "--min-rows",
    type=click.IntRange(0,),
    required=True,
    default=50_000,
    help="Min rows in the DataFrames",
)
@click.option(
    "--max-rows", type=int, required=True, default=1_000_000, help="Max rows in the DataFrames"
)
@click.option(
    "--num-examples", type=int, required=True, default=10, help="How many Dataframes to run"
)
def main(func, num_cols, min_rows, max_rows, num_examples):
    """
    Will generate `num-examples` of DataFrames using numpy.linspace, going from `min-rows` rows to
    `max-rows` rows.
    """
    if func == "readsql":
        return
    try:
        # run benchmarks
        docker_generator, creds = setup()
        results = []
        for n in np.linspace(min_rows, max_rows, num=num_examples):
            num_rows = int(n)
            df = pd.DataFrame(
                data=np.ndarray(shape=(num_rows, num_cols), dtype=int),
                columns=[f"col-{x}" for x in range(num_cols)],
            )
            _results = run_benchmark(df=df, creds=creds)
            results.append({"num_rows": num_rows, **_results})
    finally:
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
    # https://stackoverflow.com/a/44444489/6067848
    plot.set_xticklabels([f"{x:,.0f}" for x in plot.get_xticks()])
    plot.get_figure().savefig(plot_file)
    env_info = gather_env_info()
    with open(env_file, "wt") as file:
        json.dump(env_info, file, indent=2)


if __name__ == "__main__":
    cli()
