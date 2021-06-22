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

def test_csql_calc():
    #
    # Test 2: function in-query
    #
    pr = Prosto("My Prosto")

    table_csql = "TABLE  My_table (A) FUNC lambda **m: pd.DataFrame({'A': [1, 2, 3]})"
    calc_csql = "CALC  My_table (A) -> new_column FUNC lambda x: float(x)"

    translate_column_sql(pr, table_csql)
    translate_column_sql(pr, calc_csql)

    assert pr.get_table("My_table")
    assert pr.get_column("My_table", "new_column")

    pr.run()

    assert list(pr.get_table("My_table").get_series('new_column')) == [1.0, 2.0, 3.0]

    #
    # Test 2: function by-reference
    #
    pr = Prosto("My Prosto")

    df = pd.DataFrame({'A': [1, 2, 3]})  # Use FUNC "lambda **m: df" (df cannot be resolved during population)

    table_csql = "TABLE  My_table (A)"
    calc_csql = "CALC  My_table (A) -> new_column"

    translate_column_sql(pr, table_csql, lambda **m: df)
    translate_column_sql(pr, calc_csql, lambda x: float(x))

    assert pr.get_table("My_table")
    assert pr.get_column("My_table", "new_column")

    pr.run()

    assert list(pr.get_table("My_table").get_series('new_column')) == [1.0, 2.0, 3.0]


def test_csql_roll():
    pr = Prosto("My Prosto")

    df = pd.DataFrame({'A': [1.0, 2.0, 3.0]})

    table_csql = "TABLE  My_table (A)"
    calc_csql = "ROLL  My_table (A) -> new_column WINDOW 2"

    translate_column_sql(pr, table_csql, lambda **m: df)
    translate_column_sql(pr, calc_csql, lambda x: x.sum())

    assert pr.get_table("My_table")
    assert pr.get_column("My_table", "new_column")

    pr.run()

    assert list(pr.get_table("My_table").get_series('new_column')) == [None, 3.0, 5.0]


def test_csql_link():
    pr = Prosto("My Prosto")

    facts_df = pd.DataFrame({'A': ['a', 'a', 'b', 'b']})
    groups_df = pd.DataFrame({'A': ['a', 'b', 'c']})

    facts_csql = "TABLE  Facts (A)"
    groups_csql = "TABLE  Groups (A)"
    link_csql = "LINK  Facts (A) -> new_column -> Groups (A)"

    translate_column_sql(pr, facts_csql, lambda **m: facts_df)
    translate_column_sql(pr, groups_csql, lambda **m: groups_df)

    translate_column_sql(pr, link_csql)

    assert pr.get_table("Facts")
    assert pr.get_table("Groups")
    assert pr.get_column("Facts", "new_column")

    pr.run()

    assert list(pr.get_table("Facts").get_series('new_column')) == [0, 0, 1, 1]


def test_csql_project():
    pr = Prosto("My Prosto")

    facts_df = pd.DataFrame({'A': ['a', 'a', 'b', 'b']})

    facts_csql = "TABLE  Facts (A)"
    project_csql = "PROJECT  Facts (A) -> new_column -> Groups (A)"

    translate_column_sql(pr, facts_csql, lambda **m: facts_df)
    translate_column_sql(pr, project_csql)

    assert pr.get_table("Facts")
    assert pr.get_table("Groups")
    assert pr.get_column("Facts", "new_column")

    pr.run()

    assert len(pr.get_table("Groups").get_df()) == 2
    assert len(pr.get_table("Groups").get_df().columns) == 1
    assert list(pr.get_table("Facts").get_series('new_column')) == [0, 0, 1, 1]


def test_csql_aggregate():
    pr = Prosto("My Prosto")

    facts_df = pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0], 'N': [4.0, 3.0, 2.0, 1.0]})
    groups_df = pd.DataFrame({'A': ['a', 'b', 'c']})

    facts_csql = "TABLE  Facts (A, M, N)"
    groups_csql = "TABLE  Groups (A)"

    link_csql = "LINK  Facts (A) -> new_column -> Groups (A)"
    agg_csql = "AGGREGATE  Facts (M) -> new_column -> Groups (Aggregate)"

    translate_column_sql(pr, facts_csql, lambda **m: facts_df)
    translate_column_sql(pr, groups_csql, lambda **m: groups_df)

    translate_column_sql(pr, link_csql)
    translate_column_sql(pr, agg_csql, lambda x, bias, **model: x.sum() + bias, {"bias": 0.0})

    assert pr.get_table("Facts")
    assert pr.get_table("Groups")
    assert pr.get_column("Facts", "new_column")

    pr.run()

    assert list(pr.get_table("Groups").get_series('Aggregate')) == [3.0, 7.0, 0.0]
