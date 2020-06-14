from setuptools import find_packages, setup

with open("./README.md", "r") as file:
    long_description = file.read()


setup(
    name="bcpandas",
    version="1.0.0",
    author="yehoshuadimarsky",
    description="High-level wrapper around BCP for high performance data transfers between pandas and SQL Server. No knowledge of BCP required!!",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yehoshuadimarsky/bcpandas",
    packages=find_packages(),
    install_requires=["pandas>=0.19", "pyodbc", "sqlalchemy>=1.1.4"],
    python_requires=">=3.6, <3.9",
    keywords="bcp mssql pandas",
    classifiers=[
        "Topic :: Database",
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
