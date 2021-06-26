import pytest

from prosto.Prosto import *
from prosto.column_sql import *


def test_filter_table():
    ctx = Prosto("My Prosto")

    tbl = ctx.populate(
        table_name="Base table", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0], 'B': ['x', 'yy', 'zzz']})", tables=[]
    )

    # This (boolean) column will be used for filtering
    clm = ctx.compute(
        name="filter_column", table=tbl.id,
        func="lambda x, param: (x['A'] > param) & (x['B'].str.len() < 3)",  # Return a boolean Series
        columns=["A", "B"], model={"param": 1.5}
    )

    tbl.evaluate()
    clm.evaluate()

    tbl = ctx.filter(
        table_name="Filtered table", attributes=["super"],
        func=None, tables=["Base table"], columns=["filter_column"]
    )

    tbl.evaluate()

    assert len(tbl.get_df().columns) == 1  # Only one link-attribute is created
    assert len(tbl.get_df()) == 1
    assert tbl.get_df()['super'][0] == 1

    #
    # Test topology
    #
    topology = Topology(ctx)
    topology.translate()
    layers = topology.elem_layers

    assert len(layers) == 3

    assert set([x.id for x in layers[0]]) == {"Base table"}
    assert set([x.id for x in layers[1]]) == {"filter_column"}
    assert set([x.id for x in layers[2]]) == {"Filtered table"}


def test_filter_inheritance():
    """Test topology augmentation. Use columns from the parent table by automatically adding the merge operation to topology."""
    ctx = Prosto("My Prosto")

    base_tbl = ctx.populate(
        table_name="Base table", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0], 'B': ['x', 'yy', 'zzz']})", tables=[]
    )

    # This (boolean) column will be used for filtering
    clm = ctx.compute(
        name="filter_column", table=base_tbl.id,
        func="lambda x, param: (x['A'] > param) & (x['B'].str.len() < 3)",  # Return a boolean Series
        columns=["A", "B"], model={"param": 1.5}
    )

    f_tbl = ctx.filter(
        table_name="Filtered table", attributes=["super"],
        func=None, tables=["Base table"], columns=["filter_column"]
    )

    # In this calculate column, we use a column of the filtered table which actually exists only in the base table
    clm = ctx.calculate(
        name="My column", table=f_tbl.id,
        func="lambda x: x + 1.0", columns=["A"], model=None
    )

    ctx.run()

    clm_data = f_tbl.get_series('My column')

    assert np.isclose(len(clm_data), 1)
    assert np.isclose(clm_data[0], 3.0)

    # This column had to be added automatically by the augmentation procedure
    # It is inherited from the base table and materialized via merge operation
    # It stores original values of the inherited base column
    clm_data = f_tbl.get_series('A')
    assert np.isclose(clm_data[0], 2)


def test_filter_csql():
    ctx = Prosto("My Prosto")

    base_df = pd.DataFrame({'A': [1.0, 2.0, 3.0], 'B': ['x', 'yy', 'zzz']})

    base_csql = "TABLE  Base (A, B)"
    calc_csql = "CALC  Base (A, B) -> filter_column"
    filter_csql = "FILTER Base (filter_column) -> super -> Filtered"

    translate_column_sql(ctx, base_csql, lambda **m: base_df)
    translate_column_sql(ctx, calc_csql, lambda x, param: (x['A'] > param) & (len(x['B']) < 3), {"param": 1.5})
    translate_column_sql(ctx, filter_csql)

    assert ctx.get_table("Base")
    assert ctx.get_table("Filtered")

    ctx.run()

    assert list(ctx.get_table("Filtered").get_series('super')) == [1]
