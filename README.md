# bcpandas

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PyPI version](https://img.shields.io/pypi/v/bcpandas.svg)](https://pypi.org/project/bcpandas/)
[![Conda-Forge version](https://img.shields.io/conda/vn/conda-forge/bcpandas.svg)](https://anaconda.org/conda-forge/bcpandas)
[![GitHub license](https://img.shields.io/github/license/yehoshuadimarsky/bcpandas.svg)](https://github.com/yehoshuadimarsky/bcpandas/blob/master/LICENSE)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/bcpandas.svg)](https://pypi.python.org/pypi/bcpandas/)
[![PyPI status](https://img.shields.io/pypi/status/bcpandas.svg)](https://pypi.python.org/pypi/bcpandas/)
[![Awesome Badges](https://img.shields.io/badge/badges-awesome-green.svg)](https://github.com/Naereen/badges)


High-level wrapper around BCP for high performance data transfers between pandas and SQL Server. No knowledge of BCP required!!

## Quickstart

```python
In [1]: import pandas as pd
   ...: import numpy as np
   ...: 
   ...: from bcpandas import SqlCreds, to_sql, read_sql

In [2]: creds = SqlCreds(
   ...:     'my_server',
   ...:     'my_db',
   ...:     'my_username',
   ...:     'my_password'
   ...: )

In [3]: df = pd.DataFrame(
   ...:         data=np.ndarray(shape=(10, 6), dtype=int), 
   ...:         columns=[f"col_{x}" for x in range(6)]
   ...:     )

In [4]: df
Out[4]: 
     col_0    col_1    col_2    col_3    col_4    col_5
0  4128860  6029375  3801155  5570652  6619251  7536754
1  4849756  7536751  4456552  7143529  7471201  7012467
2  6029433  6881357  6881390  7274595  6553710  3342433
3  6619228  7733358  6029427  6488162  6357104  6553710
4  7536737  7077980  6422633  7536732  7602281  2949221
5  6357104  7012451  6750305  7536741  7340124  7274610
6  7340141  6226036  7274612  7077999  6881387  6029428
7  6619243  6226041  6881378  6553710  7209065  6029415
8  6881378  6553710  7209065  7536743  7274588  6619248
9  6226030  7209065  6619231  6881380  7274612  3014770

In [5]: to_sql(df, 'my_test_table', creds, index=False, if_exists='replace')

In [6]: df2 = read_sql('my_test_table', creds)

In [7]: df2
Out[7]: 
     col_0    col_1    col_2    col_3    col_4    col_5
0  4128860  6029375  3801155  5570652  6619251  7536754
1  4849756  7536751  4456552  7143529  7471201  7012467
2  6029433  6881357  6881390  7274595  6553710  3342433
3  6619228  7733358  6029427  6488162  6357104  6553710
4  7536737  7077980  6422633  7536732  7602281  2949221
5  6357104  7012451  6750305  7536741  7340124  7274610
6  7340141  6226036  7274612  7077999  6881387  6029428
7  6619243  6226041  6881378  6553710  7209065  6029415
8  6881378  6553710  7209065  7536743  7274588  6619248
9  6226030  7209065  6619231  6881380  7274612  3014770
```
## Benchmarks
_# TODO_

## Requirements
### Database
Any version of Microsoft SQL Server. Can be installed on-prem, in the cloud, on a VM, or the Azure SQL Database/Data Warehouse versions.
### Python User
- [BCP](https://docs.microsoft.com/en-us/sql/tools/bcp-utility) Utility
- [SqlCmd](https://docs.microsoft.com/en-us/sql/tools/sqlcmd-utility) Utility
- Microsoft ODBC Driver **11, 13, 13.1, or 17** for SQL Server. See the [pyodbc docs](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Windows) for details.
- Python >= 3.6
- `pandas` >= 0.19
- `sqlalchemy` >= 1.1.4
- `pyodbc` as the [supported DBAPI](https://docs.sqlalchemy.org/en/lastest/dialects/mssql.html#dialect-mssql)
- Windows as the client OS
  - Linux and MacOS are theoretically compatible, but never tested

## Installation
Source | Command
:---: | :---:
PyPI | ```pip install bcpandas``` 
Conda| ```conda install -c conda-forge bcpandas```

## Usage

### Recommended Usage
_# TODO_ When to use bcpandas vs. regular pandas.

### Credential/Connection object
Bcpandas uses a simple username/password authentication for the BCP and SqlCmd utilities, but it may also require a full `sqlalchemy.Engine` object like the regular pandas API expects, for:
1. Creating the SQL table in `to_sql` and `if_exists='replace`, because bcpandas uses some of the internal pandas code to do this
2. If the bcpandas operation fails, the user can specify that the operation should be retried using the regular pandas methods, which require a full `Engine` object.
_(Planned for in later versions)_

Therefore, the user has 2 choices. 
1. Pass a full `Engine` object to the bcpandas `SqlCreds` object. Bcpandas will attempt to parse out the server, database, username, and password to pass to the command line utilities. If a DSN is used, this will fail.
2. Create the bcpandas `SqlCreds` object with just the minimum attributes needed (server, database, username, password), and have bcpandas create a full `Engine` object from this. In this case, bcpandas will use `pyodbc` and `sqlalchemy`, and rely on the Microsoft ODBC Driver for SQL Server.


## Limitations

Here are some caveats and limitations of bcpandas. Hopefully they will be addressed in future releases
* In the `to_sql` function:
  * If `replace` is passed to the `if_exists` parameter, the new SQL table created will make the columns all of `NVARCHAR(MAX)` type.
  * If `append` is passed to the `if_exists` parameter, if the dataframe columns don't match the SQL table columns exactly by both name and order, it will fail.
  * If there is a NaN/Null in the last column of the dataframe it will throw an error. This is due to a BCP issue. See my issue with Microsoft about this [here](https://github.com/MicrosoftDocs/sql-docs/issues/2689) .
  * Bcpandas attempts to use a delimiter that is not present in the dataframe. This is because BCP does __not__ ignore delimiter characters when surrounded by quotes, unlike CSVs - see [here](https://docs.microsoft.com/en-us/sql/relational-databases/import-export/specify-field-and-row-terminators-sql-server#characters-supported-as-terminators) in the Microsoft docs. Therefore, if all possible delimiter characters are present in the dataframe and bcpandas cannot find a delimiter to use, it will throw an error.
    * Possible delimiters are specified in `constants.py` .
* Currently the STDOUT stream from BCP and SqlCmd is not asynchronous.

## Background
Reading and writing data from pandas DataFrames to/from a SQL database is very slow using the built-in `read_sql` and `to_sql` methods, even with the newly introduced [`execute_many`](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method) option. For Microsoft SQL Server, a far far faster method is to use the BCP utility provided by Microsoft. This utility is a command line tool that transfers data to/from the database and flat text files.

This package is a wrapper for seamlessly using the bcp utility from Python using a pandas DataFrame. Despite the IO hits, the fastest option by far is saving the data to a CSV file in the file system and using the bcp utility to transfer the CSV file to SQL Server. **Best of all, you don't need to know anything about using BCP at all!**

### Existing Solutions
> Much credit is due to `bcpy` for the original idea and for some of the code that was adopted and changed.
<details>
  <summary>bcpy</summary>

  [bcpy](https://github.com/titan550/bcpy) has several flaws:
  * No support for reading from SQL, only writing to SQL
  * A convoluted, overly class-based internal design
  * Scope a bit too broad - deals with pandas as well as flat files
  This repository aims to fix and improve on `bcpy` and the above issues by making the design choices described earlier.
</details>


<details>
  <summary>magical-sqlserver</summary>
  
  [magical-sqlserver](https://github.com/brennoflavio/magical-sqlserver) is a library to make working with Python and SQL Server very easy. But it doesn't fit what I'm trying to do:
  * No built in support for pandas DataFrame
  * Larger codebase, I'm not fully comfortable with the dependency on the very heavy pymssql

</details>

### Design and Scope
The _**only**_ scope of `bcpandas` is to read and write between a pandas DataFrame and a Microsoft SQL Server database. That's it. We do _**not**_ concern ourselves with reading existing flat files to/from SQL - that introduces _way_ to much complexity in trying to parse and decode the various parts of the file, like delimiters, quote characters, and line endings. Instead, to read/write an exiting flat file, just import it via pandas into a DataFrame, and then use `bcpandas`.

The big benefit of this is that we get to precicely control all the finicky parts of the text file when we write/read it to a local file and then in the BCP utility. This lets us set library-wide defaults (maybe configurable in the future) and work with those.

For now, we are using the non-XML BCP format file type. In the future, XML format files may be added.


## Testing
Testing uses `pytest`. A local SQL Server is spun up using Docker.

## Contributing
Please, all contributions are very welcome! 

I will attempt to use the `pandas` docstring style as detailed [here](https://pandas.pydata.org/pandas-docs/stable/development/contributing_docstring.html).
