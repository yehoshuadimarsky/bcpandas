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

## Requirements
- BCP Utility
    - [Windows](https://docs.microsoft.com/en-us/sql/tools/bcp-utility)
- SqlCmd Utility
    - [Windows](https://docs.microsoft.com/en-us/sql/tools/sqlcmd-utility)
- python >= 3.6
- pandas

## Motivations and Design
### Overview
Reading and writing data from pandas DataFrames to/from a SQL database is very slow using the built-in `read_sql` and `to_sql` methods, even with the newly introduced `execute_many` option. For Microsoft SQL Server, a far far faster method is to use the BCP utility provided by Microsoft. This utility is a command line tool that transfers data to/from the database and flat text files.

This package is a wrapper for seamlessly using the bcp utility from Python using a pandas DataFrame. Despite the IO hits, the fastest option by far is saving the data to a CSV file in the file system and using the bcp utility to transfer the CSV file to SQL Server. **Best of all, you don't need to know anything about using BCP at all!**

### Existing Solutions

<table>
<tr>
  <td><b>Name</b></td>
  <td><b>GitHub</b></td>
  <td><b>PyPI</b></td>
</tr>
<tr>
  <td>bcpy</td>
  <td>https://github.com/titan550/bcpy</td>
  <td>https://pypi.org/project/bcpy</td>
</tr>
<tr>
  <td>magical-sqlserver</td>
  <td>https://github.com/brennoflavio/magical-sqlserver</td>
  <td>https://pypi.org/project/magical-sqlserver/</td>
</tr>
</table>

#### bcpy
`bcpy` has several flaws:
* No support for reading from SQL, only writing to SQL
* A convoluted, overly class-based internal design
* Scope a bit too broad - deals with pandas as well as flat files

This repository aims to fix and improve on `bcpy` and the above issues by making the design choices described below.

> Note, much credit is due to `bcpy` for the original idea and for some of the code that was adopted and changed.

#### magical-sqlserver
`magical-sqlserver` is a library to make working with Python and SQL Server very easy. But it doesn't fit what I'm trying to do:
* No built in support for pandas DataFrame
* Larger codebase, I'm not fully comfortable with the dependency on the very heavy pymssql



### Design and Scope
The _**only**_ scope of `bcpandas` is to read and write between a pandas DataFrame and a Microsoft SQL Server database. That's it. We do _**not**_ concern ourselves with reading existing flat files to/from SQL - that introduces _way_ to much complexity in trying to parse and decode the various parts of the file, like delimiters, quote characters, and line endings. Instead, to read/write an exiting flat file, just import it via pandas into a DataFrame, and then use `bcpandas`.

The big benefit of this is that we get to precicely control all the finicky parts of the text file when we write/read it to a local file and then in the BCP utility. This lets us set library-wide defaults (maybe configurable in the future) and work with those.

For now, we are using the non-XML BCP format file type. In the future, XML format files may be added.

Currently, this is being built with only Windows in mind. Linux support is definitely easily added, it's just not in the immediate scope of the project yet. PRs are welcome.

Finally, the SQL Server databases supported are both the on-prem and Azure versions.

## Benchmarks
_# TODO_

## Installation
You can download and install this package from PyPI

```
pip install bcpandas
```

or from conda
```
conda install -c conda-forge bcpandas
```

## Contributing
Please, all contributions are very welcome! 

I will attempt to use the `pandas` code style as detailed [here](https://pandas.pydata.org/pandas-docs/stable/development/contributing_docstring.html).
