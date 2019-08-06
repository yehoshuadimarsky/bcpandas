from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="bcpandas",
    version="0.1.2",
    author="Josh Dimarsky",
    description="High-level wrapper around BCP for high performance data transfers between pandas and SQL Server. No knowledge of BCP required!!",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yehoshuadimarsky/bcpandas",
    packages=find_packages(exclude=["tests.*", "tests"]),
    python_requires=">=3.6",
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
