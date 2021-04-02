import pytest

from prosto.Prosto import *

def test_filter_table():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="Base table", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0], 'B': ['x', 'yy', 'zzz']})", tables=[]
    )

    # This (boolean) column will be used for filtering
    clm = sch.compute(
        name="filter_column", table=tbl.id,
        func="lambda x, param: (x['A'] > param) & (x['B'].str.len() < 3)",  # Return a boolean Series
        columns=["A", "B"], model={"param": 1.5}
    )

    tbl.evaluate()
    clm.evaluate()

    tbl = sch.filter(
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
    topology = Topology(sch)
    topology.translate()
    layers = topology.elem_layers

    assert len(layers) == 3

    assert set([x.id for x in layers[0]]) == set(["Base table"])
    assert set([x.id for x in layers[1]]) == set(["filter_column"])
    assert set([x.id for x in layers[2]]) == set(["Filtered table"])

def test_filter_inheritance():
    """Test topology augmentation. Use columns from the parent table by automatically adding the merge operation to topology."""
    sch = Prosto("My Prosto")

    base_tbl = sch.populate(
        table_name="Base table", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0], 'B': ['x', 'yy', 'zzz']})", tables=[]
    )

    # This (boolean) column will be used for filtering
    clm = sch.compute(
        name="filter_column", table=base_tbl.id,
        func="lambda x, param: (x['A'] > param) & (x['B'].str.len() < 3)",  # Return a boolean Series
        columns=["A", "B"], model={"param": 1.5}
    )

    f_tbl = sch.filter(
        table_name="Filtered table", attributes=["super"],
        func=None, tables=["Base table"], columns=["filter_column"]
    )

    # In this calculate column, we use a column of the filtered table which actually exists only in the base table
    clm = sch.calculate(
        name="My column", table=f_tbl.id,
        func="lambda x: x + 1.0", columns=["A"], model=None
    )

    sch.run()

    clm_data = f_tbl.get_series('My column')

    assert np.isclose(len(clm_data), 1)
    assert np.isclose(clm_data[0], 3.0)

    # This column had to be added automatically by the augmentation procedure
    # It is inherited from the base table and materialized via merge operation
    # It stores original values of the inherited base column
    clm_data = f_tbl.get_series('A')
    assert np.isclose(clm_data[0], 2)
