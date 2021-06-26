import pytest

from prosto.Prosto import *
from prosto.column_sql import *


def test_aggregate():
    ctx = Prosto("My Prosto")

    # Facts
    f_tbl = ctx.populate(
        table_name="Facts", attributes=["A", "M"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0], 'N': [4.0, 3.0, 2.0, 1.0]})", tables=[]
    )

    # Groups
    df = pd.DataFrame({'A': ['a', 'b', 'c']})
    g_tbl = ctx.populate(
        table_name="Groups", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c']})", tables=[]
    )

    # Link
    l_clm = ctx.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A"], linked_columns=["A"]
    )

    # Aggregation
    a_clm = ctx.aggregate(
        name="Aggregate", table=g_tbl.id,
        tables=["Facts"], link="Link",
        func="lambda x, bias, **model: x.sum() + bias", columns=["M"], model={"bias": 0.0}
    )

    f_tbl.evaluate()
    g_tbl.evaluate()

    l_clm.evaluate()
    a_clm.evaluate()

    g_tbl_data = g_tbl.get_df()
    assert len(g_tbl_data) == 3  # Same number of rows
    assert len(g_tbl_data.columns) == 2  # One aggregate column was added (and one technical "id" column was added which might be removed in future)

    a_clm_data = g_tbl.get_series('Aggregate')
    assert a_clm_data[0] == 3.0
    assert a_clm_data[1] == 7.0
    assert a_clm_data[2] == 0.0

    #
    # Test topology
    #
    topology = Topology(ctx)
    topology.translate()  # All data will be reset
    layers = topology.elem_layers

    assert len(layers) == 3

    assert set([x.id for x in layers[0]]) == {"Facts", "Groups"}
    assert set([x.id for x in layers[1]]) == {"Link"}
    assert set([x.id for x in layers[2]]) == {"Aggregate"}

    ctx.run()

    a_clm_data = g_tbl.get_series('Aggregate')
    assert a_clm_data[0] == 3.0
    assert a_clm_data[1] == 7.0
    assert a_clm_data[2] == 0.0

    #
    # Aggregation of multiple columns
    #
    # Aggregation
    a_clm2 = ctx.aggregate(
        name="Aggregate 2", table=g_tbl.id,
        tables=["Facts"], link="Link",
        func="lambda x, my_param, **model: x['M'].sum() + x['N'].sum() + my_param", columns=["M", "N"], model={"my_param": 0.0}
    )

    #a_clm2.evaluate()
    ctx.translate()  # All data will be reset
    ctx.run()  # A new column is NOT added to the existing data frame (not clear where it is)

    a_clm2_data = g_tbl.get_series('Aggregate 2')
    assert a_clm2_data[0] == 10.0
    assert a_clm2_data[1] == 10.0
    assert a_clm2_data[2] == 0.0


def test_aggregate_with_path():
    """Aggregation with column paths as measures which have to be automatically produce merge operation."""
    ctx = Prosto("My Prosto")

    # Facts
    f_tbl = ctx.populate(
        table_name="Facts", attributes=["A", "M"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0]})", tables=[]
    )

    # Groups
    df = pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [3.0, 2.0, 1.0]})
    g_tbl = ctx.populate(
        table_name="Groups", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [3.0, 2.0, 1.0]})", tables=[]
    )

    # Link
    l_clm = ctx.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A"], linked_columns=["A"]
    )

    # Aggregation
    a_clm = ctx.aggregate(
        name="Aggregate", table=g_tbl.id,
        tables=["Facts"], link="Link",
        func="lambda x, bias, **model: x.sum() + bias", columns=["Link::B"], model={"bias": 0.0}
    )

    ctx.run()

    a_clm_data = g_tbl.get_series('Aggregate')
    assert a_clm_data[0] == 6.0
    assert a_clm_data[1] == 4.0
    assert a_clm_data[2] == 0.0


def test_aggregate_csql():
    ctx = Prosto("My Prosto")

    facts_df = pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0], 'N': [4.0, 3.0, 2.0, 1.0]})
    groups_df = pd.DataFrame({'A': ['a', 'b', 'c']})

    facts_csql = "TABLE  Facts (A, M, N)"
    groups_csql = "TABLE  Groups (A)"

    link_csql = "LINK  Facts (A) -> new_column -> Groups (A)"
    agg_csql = "AGGREGATE  Facts (M) -> new_column -> Groups (Aggregate)"

    translate_column_sql(ctx, facts_csql, lambda **m: facts_df)
    translate_column_sql(ctx, groups_csql, lambda **m: groups_df)

    translate_column_sql(ctx, link_csql)
    translate_column_sql(ctx, agg_csql, lambda x, bias, **model: x.sum() + bias, {"bias": 0.0})

    assert ctx.get_table("Facts")
    assert ctx.get_table("Groups")
    assert ctx.get_column("Facts", "new_column")

    ctx.run()

    assert list(ctx.get_table("Groups").get_series('Aggregate')) == [3.0, 7.0, 0.0]
