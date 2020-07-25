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


"""
Function resolution.
"""

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
