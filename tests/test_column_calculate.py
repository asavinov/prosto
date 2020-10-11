import pytest

from prosto.Prosto import *

def test_calculate_value():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="My table", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1, 2, 3]})", tables=[]
    )

    clm = sch.calculate(
        name="My column", table=tbl.id,
        func="lambda x: float(x)", columns=["A"], model=None
    )

    tbl.evaluate()
    clm.evaluate()

    clm_data = tbl.get_column_data('My column')
    v0 = clm_data[0]
    v1 = clm_data[1]
    v2 = clm_data[2]

    assert np.isclose(v0, 1.0)
    assert np.isclose(v1, 2.0)
    assert np.isclose(v2, 3.0)

    assert isinstance(v0, float)
    assert isinstance(v1, float)
    assert isinstance(v2, float)

def test_compute():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="My table", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1, 2, 3]})", tables=[]
    )

    clm = sch.compute(
        name="My column", table=tbl.id,
        func="lambda x, **model: x.shift(**model)", columns=["A"], model={"periods": -1}
    )

    tbl.evaluate()
    clm.evaluate()

    clm_data = tbl.get_column_data('My column')
    assert np.isclose(clm_data[0], 2.0)
    assert np.isclose(clm_data[1], 3.0)
    assert pd.isna(clm_data[2])

    #
    # Test topology
    #
    topology = Topology(sch)
    topology.translate()  # All data will be reset
    layers = topology.elem_layers

    assert len(layers) == 2

    assert set([x.id for x in layers[0]]) == set(["My table"])
    assert set([x.id for x in layers[1]]) == set(["My column"])

    sch.run()

    clm_data = tbl.get_column_data('My column')
    assert np.isclose(clm_data[0], 2.0)
    assert np.isclose(clm_data[1], 3.0)
    assert pd.isna(clm_data[2])

def test_calculate_with_path():
    """Test topology augmentation. Calculation with column paths which have to be automatically produce merge operation."""
    sch = Prosto("My Prosto")

    # Facts
    f_tbl = sch.populate(
        table_name="Facts", attributes=["A", "M"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0]})", tables=[]
    )

    # Groups
    df = pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [3.0, 2.0, 1.0]})
    g_tbl = sch.populate(
        table_name="Groups", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [3.0, 2.0, 1.0]})", tables=[]
    )

    # Link
    l_clm = sch.link(
        name="Link", table=f_tbl.id, type=g_tbl.id,
        columns=["A"], linked_columns=["A"]
    )

    # Calculate
    clm = sch.calculate(
        name="My column", table=f_tbl.id,
        func="lambda x: x['M'] + x['Link::B']", columns=["M", "Link::B"], model=None
    )

    sch.run()

    clm_data = f_tbl.get_column_data('My column')
    assert clm_data[0] == 4.0
    assert clm_data[1] == 5.0
    assert clm_data[2] == 5.0
    assert clm_data[3] == 6.0

    pass
