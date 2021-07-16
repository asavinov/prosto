import pytest

from prosto.Prosto import *
from prosto.column_sql import *


def test_calculate_value():
    ctx = Prosto("My Prosto")

    tbl = ctx.populate(
        table_name="My table", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1, 2, 3]})", tables=[]
    )

    clm = ctx.calculate(
        name="My column", table=tbl.id,
        func="lambda x: float(x)", columns=["A"], model=None
    )

    tbl.evaluate()
    clm.evaluate()

    clm_data = tbl.get_series('My column')
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
    ctx = Prosto("My Prosto")

    tbl = ctx.populate(
        table_name="My table", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1, 2, 3]})", tables=[]
    )

    clm = ctx.compute(
        name="My column", table=tbl.id,
        func="lambda x, **model: x.shift(**model)", columns=["A"], model={"periods": -1}
    )

    tbl.evaluate()
    clm.evaluate()

    clm_data = tbl.get_series('My column')
    assert np.isclose(clm_data[0], 2.0)
    assert np.isclose(clm_data[1], 3.0)
    assert pd.isna(clm_data[2])

    #
    # Test topology
    #
    topology = Topology(ctx)
    topology.translate()  # All data will be reset
    layers = topology.elem_layers

    assert len(layers) == 2

    assert set([x.id for x in layers[0]]) == {"My table"}
    assert set([x.id for x in layers[1]]) == {"My column"}

    ctx.run()

    clm_data = tbl.get_series('My column')
    assert np.isclose(clm_data[0], 2.0)
    assert np.isclose(clm_data[1], 3.0)
    assert pd.isna(clm_data[2])


def test_calculate_with_path():
    """Test topology augmentation. Calculation with column paths which have to be automatically produce merge operation."""
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

    # Calculate
    clm = ctx.calculate(
        name="My column", table=f_tbl.id,
        func="lambda x: x['M'] + x['Link::B']", columns=["M", "Link::B"], model=None
    )

    ctx.run()

    clm_data = f_tbl.get_series('My column')
    assert clm_data[0] == 4.0
    assert clm_data[1] == 5.0
    assert clm_data[2] == 5.0
    assert clm_data[3] == 6.0


def test_calc_csql():
    #
    # Test 2: function in-query
    #
    ctx = Prosto("My Prosto")

    ctx.column_sql("TABLE  My_table (A) FUNC lambda **m: pd.DataFrame({'A': [1, 2, 3]})")
    ctx.column_sql("CALCULATE  My_table (A) -> new_column FUNC lambda x: float(x)")

    assert ctx.get_table("My_table")
    assert ctx.get_column("My_table", "new_column")

    ctx.run()

    assert list(ctx.get_table("My_table").get_series('new_column')) == [1.0, 2.0, 3.0]

    #
    # Test 2: function by-reference
    #
    ctx = Prosto("My Prosto")

    df = pd.DataFrame({'A': [1, 2, 3]})  # Use FUNC "lambda **m: df" (df cannot be resolved during population)

    ctx.column_sql("TABLE  My_table (A)", df)
    ctx.column_sql("CALCULATE My_table (A) -> new_column", lambda x: float(x))

    assert ctx.get_table("My_table")
    assert ctx.get_column("My_table", "new_column")

    ctx.run()

    assert list(ctx.get_table("My_table").get_series('new_column')) == [1.0, 2.0, 3.0]
