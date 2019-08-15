# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 22:05:04 2019

@author: JoshDimarsky
"""

import json

import click
from jinja2 import Environment, FileSystemLoader
from github import Github


@click.group()
def cli():
    pass


@cli.command()
@click.option("--draft", is_flag=True)
def github_release(draft):
    print("getting auth and config")
    with open("./creds.json") as file:
        auth = json.load(file)

    with open("./dist.json") as file:
        config = json.load(file)
    print("auth and config loaded")

    print("logging into GitHub")
    g = Github(auth["github_token"])
    repo = g.get_repo(f"{config['GH_user']}/{config['name']}")
    master_branch = repo.get_branch("master")

    print("Creating release")
    # https://pygithub.readthedocs.io/en/latest/github_objects/Repository.html#github.Repository.Repository.create_git_release
    repo.create_git_release(
        tag=config["version"],
        name=config["GH_release_name"],
        message=config["GH_release_message"],
        draft=draft,
        prerelease=False,
        target_commitish=master_branch,
    )
    print("All done!")


@cli.command()
@click.option("--sha256", required=True, type=str)
def render_conda(sha256):
    print("jinja rendering conda template file called meta.template.yaml ...")
    with open("./dist.json", "r") as file:
        config = json.load(file)

    env = Environment(loader=FileSystemLoader("."))
    t = env.get_template("meta.template.yaml")

    rendered = t.render(
        name=config["name"],
        version=config["version"],
        sha256val=sha256,
        creator=config["GH_user"],
        python_version=config["python_version"],
        dependencies=config["dependencies"],
        dependencies_test=config["dependencies_test"],
        PYTHON="{{ PYTHON }}",
    )
    with open("./meta.yaml", "wt") as yaml_file:
        yaml_file.write(rendered)
    print("all done - file rendered and saved as meta.yaml")


if __name__ == "__main__":
    cli()
