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

#
# Function resolution
#

def resolve_full_name(full_name):
    """
    Resolve the specified name or definition of the function to a reference.
    Fully qualified name consists of module name and function name separated by a colon, for example:  'mod1.mod2.mod3:class1.class2.func1.func2'.
    """

    if not full_name:
        return None

    elif isinstance(full_name, (types.FunctionType, types.BuiltinFunctionType, functools.partial)):
        return full_name

    elif full_name.strip().startswith('lambda '):
        try:
            func = eval(full_name)
        except Exception as e:
            log.error(f"Error translating lambda function: {full_name}.")
            log.debug(e)
            return None
        return func

    elif full_name.strip().startswith('def '):
        pass  # TODO: Standard function definition

    else:  # Function name
        mod_and_func = full_name.split(':', 1)
        mod_name = mod_and_func[0] if len(mod_and_func) > 1 else None
        func_name = mod_and_func[-1]

        if mod_name:
            mod = resolve_module(mod_name)
            if mod is None: return None
            func = resolve_name_in_mod(func_name, mod)
            return func
        else:
            pass # TODO: Module is not specified. Search in all modules

    return None

def all_modules():
    modules = []
    return modules

def resolve_module(mod_name: str):
    mod = sys.modules.get(mod_name)

    if mod:
        return mod

    try:
        mod = importlib.import_module(mod_name)
    except Exception as e:
        pass

    return mod

def resolve_name_in_mod(func_name: str, mod):
    # Split full name into segments (classes and functions)
    name_path = func_name.split('.')

    # Sequentially resolve each next segment in the result of the previous segment starting from the specified module
    last_segment = mod
    for i in range(len(name_path)):
        name_segment = name_path[i]
        ref_segment = None

        try:
            ref_segment = getattr(last_segment, name_segment)
            """
            for key, val in mod.__dict__.items():
                if not inspect.isclass(val): continue

                members = inspect.getmembers(val, predicate=inspect.ismethod)  # A list of all members of the class
                for n, m in members:
                    if n == func_name: return m
            """
        except AttributeError as e:
            pass

        if ref_segment is None:
            return None
        else:
            last_segment = ref_segment

    return last_segment

def import_modules(imports):
    modules = []

    for mod_name in imports:
        mod = None
        try:
            mod = importlib.import_module(mod_name)
        except ImportError as ie:
            pass

        if mod:
            modules.append(mod)
            continue  # Module found and imported

        # Try to import from source file
        # See: https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
        try:
            file_path = mod_name.replace('.', '/')
            file_path = file_path + '.py'
            spec = importlib.util.spec_from_file_location(mod_name, file_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        except ImportError as ie:
            pass

        if mod:
            modules.append(mod)
            sys.modules[mod_name] = mod
            continue

        log.warning(f"Cannot import module '{mod_name}'. Ignored. This can cause errors later if its functions are used in the workflow")

    return modules


if __name__ == "__main__":
    pass
