from typing import Union, Any, List, Set, Dict, Tuple, Optional
import os
import sys
import types
import inspect
import importlib
import importlib.util
import functools

import pandas as pd
import numpy as np

import logging
log = logging.getLogger('prosto')


#
# Columns and frames
#

def get_columns(names, df=None) -> List[str]:
    """Produce a list of column names by also validating them against the data frame."""
    result = []

    if isinstance(names, str):  # A single column name
        result.append(names)

    elif isinstance(names, int) and df is not None:  # A single column number
        result.append(df.columns[names])

    elif isinstance(names, (list, tuple)):  # A list of column names
        for col in names:
            if isinstance(col, str):
                result.append(col)
            elif isinstance(col, int) and df is not None:
                result.append(df.columns[col])
            else:
                log.error("Error reading column '{}'. Names have to be strings or integers.".format(str(col)))
                return None

        # Process default (auto) values
        if len(result) == 0 and df is not None:  # Explicit empty list = ALL columns
           result = get_all_columns(df)

    elif isinstance(names, dict):  # An object specifying which columns to select
        exclude = names.get("exclude")
        if not isinstance(exclude, dict):
            exclude_columns = get_columns(exclude, df)
        else:
            log.error(f"Error reading column '{exclude}'. Excluded columns have to be (a list of) strings or integers.")
            return None

        # Get all columns and exclude the specified ones
        all_columns = get_all_columns(df)
        result = [x for x in all_columns if x not in exclude_columns]

    else:
        log.error(f"Column names have to be a list of strings or a string.")
        return None

    #
    # Validate
    #

    # Check that all columns are available
    if df is None:
        return result
    elif isinstance(df, pd.DataFrame):
        out = []
        for col in result:
            if col in df.columns:
                out.append(col)
            else:
                log.warning(f"Column '{str(col)}' cannot be found. Skip column.")
        return out

    elif isinstance(df, pd.core.groupby.groupby.DataFrameGroupBy):
        out = []
        for col in result:
            col_exists = False
            try:
                df.__getattr__(col)
                col_exists = True
            except:
                pass

            if col_exists:
                out.append(col)
            else:
                log.warning(f"Column '{str(col)}' cannot be found. Skip column.")
        return out

    return result

def get_all_columns(df) -> List[str]:
    if df is None:
        return []
    elif isinstance(df, pd.DataFrame):
        return df.columns.tolist()
    elif isinstance(df, pd.core.groupby.groupby.DataFrameGroupBy):
        # TODO: We need to exclude key columns which are used for gropuing
        return df.obj.columns.tolist()
    else:
        return None

def all_columns_exist(names, df) -> bool:
    all_columns_available = True
    for col in names:
        if col not in df.columns:
            all_columns_available = False
            log.warning(f"Column '{col}' is not available.")
            break
    if not all_columns_available:
        return False
    return True


if __name__ == "__main__":
    pass
