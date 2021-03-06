import pytest

from prosto.Prosto import *


def test_merge():
    ctx = Prosto("My Prosto")

    # Facts
    f_tbl = ctx.populate(
        table_name="Facts", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b']})", tables=[]
    )

    # Groups
    g_tbl = ctx.populate(
        table_name="Groups", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [1.0, 2.0, 3.0]})", tables=[]
    )

    # Link
    l_clm = ctx.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A"], linked_columns=["A"]
    )

    # Merge
    m_clm = ctx.merge("Merge", f_tbl.id, ["Link", "B"])

    f_tbl.evaluate()
    g_tbl.evaluate()

    l_clm.evaluate()
    m_clm.evaluate()

    f_tbl_data = f_tbl.get_df()
    assert len(f_tbl_data) == 4  # Same number of rows
    assert len(f_tbl_data.columns) == 3

    m_data = f_tbl.get_series("Merge")
    assert m_data[0] == 1.0
    assert m_data[1] == 1.0
    assert m_data[2] == 2.0
    assert m_data[3] == 2.0

    #
    # Test topology
    #
    topology = Topology(ctx)
    topology.translate()  # All data will be reset
    layers = topology.elem_layers

    assert len(layers) == 3

    assert set([x.id for x in layers[0]]) == {"Facts", "Groups"}
    assert set([x.id for x in layers[1]]) == {"Link"}
    assert set([x.id for x in layers[2]]) == {"Merge"}

    ctx.run()

    m_data = f_tbl.get_series("Merge")
    assert m_data.to_list() == [1.0, 1.0, 2.0, 2.0]


def test_merge_path():
    ctx = Prosto("My Prosto")

    # Facts
    f_tbl = ctx.populate(
        table_name="Facts", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b']})", tables=[]
    )

    # Groups
    g_tbl = ctx.populate(
        table_name="Groups", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [2.0, 3.0, 3.0]})", tables=[]
    )
    # Link
    l_clm = ctx.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A"], linked_columns=["A"]
    )

    # SuperGroups
    sg_tbl = ctx.populate(
        table_name="SuperGroups", attributes=["B", "C"],
        func="lambda **m: pd.DataFrame({'B': [2.0, 3.0, 4.0], 'C': ['x', 'y', 'z']})", tables=[]
    )
    # SuperLink
    sl_clm = ctx.link(
        name="SuperLink", table=g_tbl.id, type=sg_tbl.id,
        columns=["B"], linked_columns=["B"]
    )

    # Merge
    m_clm = ctx.merge("Merge", f_tbl.id, ["Link", "SuperLink", "C"])

    f_tbl.evaluate()
    g_tbl.evaluate()
    sg_tbl.evaluate()

    l_clm.evaluate()
    sl_clm.evaluate()
    m_clm.evaluate()

    f_tbl_data = f_tbl.get_df()
    assert len(f_tbl_data) == 4  # Same number of rows
    assert len(f_tbl_data.columns) == 3

    m_data = f_tbl.get_series("Merge")
    assert m_data.to_list() == ['x', 'x', 'y', 'y']

    #
    # Test topology
    #
    topology = Topology(ctx)
    topology.translate()  # All data will be reset
    layers = topology.elem_layers

    assert len(layers) == 3

    assert set([x.id for x in layers[0]]) == {"Facts", "Groups", "SuperGroups"}
    assert set([x.id for x in layers[1]]) == {"Link", "SuperLink"}
    assert set([x.id for x in layers[2]]) == {"Merge"}

    ctx.run()

    m_data = f_tbl.get_series("Merge")
    assert m_data.to_list() == ['x', 'x', 'y', 'y']


def test_merge_path2():
    """
    Here we do the same as previous test, but specify complex path using separators (rather than a list of simple segment names).
    So only the definition of merge operation changes.
    """
    ctx = Prosto("My Prosto")

    # Facts
    f_tbl = ctx.populate(
        table_name="Facts", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b']})", tables=[]
    )

    # Groups
    g_tbl = ctx.populate(
        table_name="Groups", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [2.0, 3.0, 3.0]})", tables=[]
    )
    # Link
    l_clm = ctx.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A"], linked_columns=["A"]
    )

    # SuperGroups
    sg_tbl = ctx.populate(
        table_name="SuperGroups", attributes=["B", "C"],
        func="lambda **m: pd.DataFrame({'B': [2.0, 3.0, 4.0], 'C': ['x', 'y', 'z']})", tables=[]
    )
    # SuperLink
    sl_clm = ctx.link(
        name="SuperLink", table=g_tbl.id, type=sg_tbl.id,
        columns=["B"], linked_columns=["B"]
    )

    # Merge
    m_clm = ctx.merge("Merge", f_tbl.id, ["Link::SuperLink::C"])

    ctx.run()

    f_tbl_data = f_tbl.get_df()
    assert len(f_tbl_data) == 4  # Same number of rows
    assert len(f_tbl_data.columns) == 3

    m_data = f_tbl.get_series("Merge")
    assert m_data.to_list() == ['x', 'x', 'y', 'y']
