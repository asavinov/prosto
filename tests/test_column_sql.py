import numpy as np
import pytest

from prosto.column_sql import *


def test_parser():

    table_csql = " TABLE  MyTable ( A, B ) FUNC lambda **m: df ARGS {'key': 'value'} "
    op, entries, func, args, win = parse_column_sql(table_csql)
    assert op == "TABLE"
    assert entries == [['MyTable', 'A', 'B']]
    assert func == "lambda **m: df"
    assert args == "{'key': 'value'}"

    calc_csql = " CALC  table ( col1, col2) -> new_column FUNC func_fn ARGS {'key': 'value'} "
    op, entries, func, args, win = parse_column_sql(calc_csql)
    assert op == "CALC"
    assert entries == [['table', 'col1', 'col2'], ['new_column']]
    assert func == "func_fn"
    assert args == "{'key': 'value'}"

    pass
