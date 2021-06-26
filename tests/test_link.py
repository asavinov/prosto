import pytest

from prosto.Prosto import *
from prosto.column_sql import *


def test_one_key():
    sch = Prosto("My Prosto")

    # Facts
    f_tbl = sch.populate(
        table_name="Facts", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b']})", tables=[]
    )

    # Groups
    g_tbl = sch.populate(
        table_name="Groups", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c']})", tables=[]
    )

    # Link
    l_clm = sch.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A"], linked_columns=["A"]
    )

    f_tbl.evaluate()
    g_tbl.evaluate()

    l_clm.evaluate()

    f_tbl_data = f_tbl.get_df()
    assert len(f_tbl_data) == 4  # Same number of rows
    assert len(f_tbl_data.columns) == 2

    l_data = f_tbl.get_series("Link")
    assert l_data[0] == 0
    assert l_data[1] == 0
    assert l_data[2] == 1
    assert l_data[3] == 1

    #
    # Test topology
    #
    topology = Topology(sch)
    topology.translate()  # All data will be reset
    layers = topology.elem_layers

    assert len(layers) == 2

    assert set([x.id for x in layers[0]]) == {"Facts", "Groups"}
    assert set([x.id for x in layers[1]]) == {"Link"}


def test_two_keys():
    sch = Prosto("My Prosto")

    # Facts
    f_tbl = sch.populate(
        table_name="Facts", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'b', 'a'], 'B': ['b', 'c', 'c', 'a']})", tables=[]
    )

    # Groups
    g_tbl = sch.populate(
        table_name="Groups", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'a'], 'B': ['b', 'c', 'c'], 'C': [1, 2, 3]})", tables=[]
    )

    # Link
    l_clm = sch.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A", "B"], linked_columns=["A", "B"]
    )

    f_tbl.evaluate()
    g_tbl.evaluate()

    l_clm.evaluate()

    f_tbl_data = f_tbl.get_df()
    assert len(f_tbl_data) == 4  # Same number of rows
    assert len(f_tbl_data.columns) == 3

    l_data = f_tbl.get_series("Link")
    assert l_data[0] == 0
    assert l_data[1] == 1
    assert l_data[2] == 1
    assert pd.isna(l_data[3])

    #
    # Test topology
    #
    topology = Topology(sch)
    topology.translate()  # All data will be reset
    layers = topology.elem_layers

    assert len(layers) == 2

    assert set([x.id for x in layers[0]]) == {"Facts", "Groups"}
    assert set([x.id for x in layers[1]]) == {"Link"}

    sch.run()

    l_data = f_tbl.get_series("Link")
    assert l_data[0] == 0
    assert l_data[1] == 1
    assert l_data[2] == 1
    assert pd.isna(l_data[3])


def test_link_csql():
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
