# bcpandas

:warning: :construction: This library is still under active development, should be considered in "alpha". Use at your own risk. :construction: :warning:

(That said, the source code is very small and easy to understand, so you should feel comfortable pretty quickly)

## Motivations and Design
### Overview
Reading and writing data from pandas DataFrames to/from a SQL database is very slow using the built-in `read_sql` and `to_sql` methods, even with the newly introduced `execute_many` option. For Microsoft SQL Server, a far far faster method is to use the BCP utility provided by Microsoft. This utility is a command line tool that transfers data to/from the database and flat text files.

This package is a wrapper for seamlessly using the bcp utility from Python using a pandas DataFrame. Despite the IO hits, the fastest option by far is saving the data to a CSV file in the file system and using the bcp utility to transfer the CSV file to SQL Server.

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

## Benchmarks/Comparisons
#TODO

## How Can I Install It?
You can download and install this package from PyPI

```
pip install bcpandas
```

or from conda
```
conda install -c conda-forge bcpandas
```

## Examples
#TODO

## Requirements
### General
- The BCP Utility
    - [Windows](https://docs.microsoft.com/en-us/sql/tools/bcp-utility)
- SqlCmd Utility
    - [Windows](https://docs.microsoft.com/en-us/sql/tools/sqlcmd-utility)

### Python
- pandas
- pyodbc
