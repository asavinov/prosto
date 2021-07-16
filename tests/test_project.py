import pytest

from prosto.Prosto import *
from prosto.column_sql import *


def test_one_key():
    ctx = Prosto("My Prosto")

    # Facts
    f_tbl = ctx.populate(
        table_name="Facts", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b']})", tables=[]
    )

    # Groups
    g_tbl = ctx.project(
        table_name="Groups", attributes=["X"],
        tables=["Facts"], columns=["A"]
    )

    # Link
    l_clm = ctx.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A"], linked_columns=["X"]
    )

    f_tbl.evaluate()
    g_tbl.evaluate()

    l_clm.evaluate()

    g_tbl_data = g_tbl.get_df()
    assert len(g_tbl_data) == 2
    assert len(g_tbl_data.columns) == 1

    l_data = f_tbl.get_series("Link")
    assert l_data[0] == 0
    assert l_data[1] == 0
    assert l_data[2] == 1
    assert l_data[3] == 1

    #
    # Test topology
    #
    topology = Topology(ctx)
    topology.translate()
    layers = topology.elem_layers

    assert len(layers) == 3

    assert set([x.id for x in layers[0]]) == {"Facts"}
    assert set([x.id for x in layers[1]]) == {"Groups"}
    assert set([x.id for x in layers[2]]) == {"Link"}


def test_two_keys():
    ctx = Prosto("My Prosto")

    # Facts
    f_tbl = ctx.populate(
        table_name="Facts", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'b', 'a'], 'B': ['b', 'c', 'c', 'a']})", tables=[]
    )

    # Groups
    g_tbl = ctx.project(
        table_name="Groups", attributes=["X", "Y"],
        tables=["Facts"], columns=["A", "B"]
    )

    # Link
    l_clm = ctx.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A", "B"], linked_columns=["X", "Y"]
    )

    f_tbl.evaluate()
    g_tbl.evaluate()

    l_clm.evaluate()

    g_tbl_data = g_tbl.get_df()
    assert len(g_tbl_data) == 3
    assert len(g_tbl_data.columns) == 2

    l_data = f_tbl.get_series("Link")
    assert l_data[0] == 0
    assert l_data[1] == 1
    assert l_data[2] == 1
    assert l_data[3] == 2

    #
    # Test topology
    #
    topology = Topology(ctx)
    topology.translate()
    layers = topology.elem_layers

    assert len(layers) == 3

    assert set([x.id for x in layers[0]]) == {"Facts"}
    assert set([x.id for x in layers[1]]) == {"Groups"}
    assert set([x.id for x in layers[2]]) == {"Link"}

    g_tbl_data = g_tbl.get_df()
    g_tbl_data.drop(g_tbl_data.index, inplace=True)  # Empty

    ctx.run()

    g_tbl_data = g_tbl.get_df()
    assert len(g_tbl_data) == 3
    assert len(g_tbl_data.columns) == 2


def test_csql_project():
    ctx = Prosto("My Prosto")

    facts_df = pd.DataFrame({'A': ['a', 'a', 'b', 'b']})

    ctx.column_sql("TABLE  Facts (A)", lambda **m: facts_df)
    ctx.column_sql("PROJECT  Facts (A) -> new_column -> Groups (A)")

    assert ctx.get_table("Facts")
    assert ctx.get_table("Groups")
    assert ctx.get_column("Facts", "new_column")

    ctx.run()

    assert len(ctx.get_table("Groups").get_df()) == 2
    assert len(ctx.get_table("Groups").get_df().columns) == 1
    assert list(ctx.get_table("Facts").get_series('new_column')) == [0, 0, 1, 1]
