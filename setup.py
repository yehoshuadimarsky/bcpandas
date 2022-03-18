from setuptools import find_packages, setup

with open("./README.md") as file:
    long_description = file.read()


setup(
    name="bcpandas",
    version="2.0.0",
    author="yehoshuadimarsky",
    description="High-level wrapper around BCP for high performance data transfers between pandas and SQL Server. No knowledge of BCP required!!",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yehoshuadimarsky/bcpandas",
    packages=find_packages(),
    install_requires=["pandas>=0.19", "pyodbc", "sqlalchemy>=1.0"],
    python_requires=">=3.7",
    keywords="bcp mssql pandas",
    entry_points={"pandas.sql.engine": ["bcpandas = bcpandas.main:to_sql"]},
    classifiers=[
        "Topic :: Database",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: SQL",
    ],
)
