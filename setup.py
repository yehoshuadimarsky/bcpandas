import json
from setuptools import setup, find_packages

with open("./dist.json", "r") as file:
    config = json.load(file)

with open("./README.md", "r") as file:
    long_description = file.read()


setup(
    name=config["name"],
    version=config["version"],
    author=config["author"],
    description=config["short_description"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=f"https://github.com/{config['GH_user']}/{config['name']}",
    packages=find_packages(exclude=["tests.*", "tests"]),
    python_requires=[x for x in config["dependencies"] if x.startswith("python>")],
    install_requires=[x for x in config["dependencies"] if not x.startswith("python>")],
    keywords="bcp mssql pandas",
    classifiers=[
        "Topic :: Database",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: SQL",
    ],
)
