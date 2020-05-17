import logging
import os
import warnings

from bcpandas.constants import (
    OUT,
    QUERY,
    QUERYOUT,
    SQL_TYPES,
    TABLE,
    VIEW,
    BCPandasValueError,
    read_data_settings,
)
from bcpandas.main import SqlCreds
from bcpandas.utils import bcp, get_temp_file
import pandas as pd

logger = logging.getLogger(__name__)


def read_sql(
    table_name: str,
    creds: SqlCreds,
    sql_type: str = "table",
    schema: str = "dbo",
    batch_size: int = None,
    debug: bool = False,
    delimiter: str = None,
    check_delim: bool = True,
    bcp_path: str = None,
) -> pd.DataFrame:
    """
    ** It is HIGHLY recommended to not use this method, and instead use the native pandas `read_sql*` methods. See README for details. **
    
    Reads a SQL table, view, or query into a pandas DataFrame.

    Parameters
    ----------
    table_name : str
        Name of SQL table or view, without the schema, or a query string
    creds : bcpandas.SqlCreds
        The credentials used in the SQL database.
    sql_type : {'table', 'view', 'query'}, default 'table'
        The type of SQL object that the parameter `table_name` is.
    schema : str, default 'dbo'
        The SQL schema of the table or view. If a query, will be ignored.
    batch_size : int, optional
        Rows will be read in batches of this size at a time. By default,
        all rows will be read at once.
    debug : bool, default False
        If True, will not delete the temporary CSV file, and will output its location.
    delimiter : str, optional
        One or more characters to use as a column delimiter in the temporary CSV file.
        If not supplied, the default used is specified in `constants.py` in the `read_data_settings` variable.
        **IMPORTANT** - the delimiter must not appear in the actual data in SQL or else it will fail.
    check_delim : bool, default True
        Whether to check the temporary CSV file for the presence of the delimiter in the data.
        See note below.
    bcp_path : str, default None
        The full path to the BCP utility, useful if it is not in the PATH environment variable
    
    Returns
    -------
    `pandas.DataFrame`

    Notes
    -----
    Will actually read the SQL table/view/query twice - first to get the names of the columns 
    (will only read first few rows), then all the rows using BCP.

    Also, the temporary CSV file will be read into memory twice, to check for the presence of
    the delimiter character in the data. This can cause it to take longer. If you are sure the
    delimiter isn't in the data, you can skip this check by passing `check_delim=False`
    """
    warnings.warn(
        "It is HIGHLY recommended to not use this method, and instead use the native pandas `read_sql*` methods. See README for details."
    )
    # check params
    assert sql_type in SQL_TYPES
    if batch_size == 0:
        raise BCPandasValueError("Param batch_size can't be 0")

    # set up objects
    if ";" in table_name:
        raise BCPandasValueError(
            "The SQL item cannot contain the ';' character, it interferes with getting the column names"
        )

    # read top 2 rows of query to get the columns
    logger.debug("Starting to read first 2 rows to get the column names")
    _from_clause = table_name if sql_type in (TABLE, VIEW) else f"({table_name})"

    _existing_data = pd.read_sql_query(
        sql=f"SELECT TOP 2 * FROM {_from_clause} as qry", con=creds.engine
    )

    if _existing_data.shape[0] > 0:
        cols = _existing_data.columns
        logger.debug("Successfully read the column names")
    else:
        return _existing_data

    file_path = get_temp_file()

    # set delimiter
    delim = delimiter if delimiter is not None else read_data_settings["delimiter"]
    try:
        bcp(
            sql_item=table_name,
            direction=QUERYOUT if sql_type == QUERY else OUT,
            flat_file=file_path,
            creds=creds,
            sql_type=sql_type,
            schema=schema,
            batch_size=batch_size,
            col_delimiter=delim,
            bcp_path=bcp_path,
        )
        logger.debug(f"Saved dataframe to temp CSV file at {file_path}")

        # check if delimiter is in the data more than it should be
        # there should be len(cols)-1 instances of the delimiter per row
        if check_delim:
            num_delims = len(cols) - 1
            with open(file_path, "r") as file:
                for line in file:
                    if line.count(delim) > num_delims:
                        raise BCPandasValueError(
                            f"The delimiter ({delim}) was found in the source data, cannot"
                            " import with the delimiter specified. Try specifiying a delimiter"
                            " that does not appear in the data."
                        )

        csv_kwargs = {}
        if len(delim) > 1:
            # pandas csv C engine only supports 1 character as delim
            csv_kwargs["engine"] = "python"

        return pd.read_csv(
            filepath_or_buffer=file_path,
            sep=delim,
            header=None,
            names=cols,
            index_col=False,
            **csv_kwargs,
        )
    finally:
        if not debug:
            logger.debug(f"Deleting temp CSV file")
            os.remove(file_path)
        else:
            logger.debug(
                f"`read_sql` DEBUG mode, not deleting the file. CSV file is at {file_path}."
            )
