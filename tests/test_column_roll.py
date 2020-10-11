import pytest

from prosto.Prosto import *

def test_roll_single():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="My table", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0]})", tables=[]
    )

    clm = sch.roll(
        name="Roll", table=tbl.id,
        window="2", link=None,
        func="lambda x: x.sum()", columns=["A"], model={}
    )

    tbl.evaluate()
    clm.evaluate()

    clm_data = tbl.get_column_data('Roll')

    assert pd.isna(clm_data[0])
    assert np.isclose(clm_data[1], 3.0)
    assert np.isclose(clm_data[2], 5.0)

def test_roll_multiple():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="My table", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': [1, 2, 3], 'B': [3, 2, 1]})", tables=[]
    )

    clm = sch.roll(
        name="Roll", table=tbl.id,
        window="2", link=None,
        func="lambda x: x['A'].sum() + x['B'].sum()", columns=["A", "B"], model={}
    )

    tbl.evaluate()
    clm.evaluate()

    clm_data = tbl.get_column_data('Roll')

    assert pd.isna(clm_data[0])
    assert np.isclose(clm_data[1], 8.0)
    assert np.isclose(clm_data[2], 8.0)

    #
    # Test topology
    #
    topology = Topology(sch)
    topology.translate()  # All data will be reset
    layers = topology.elem_layers

    assert len(layers) == 2

    assert set([x.id for x in layers[0]]) == set(["My table"])
    assert set([x.id for x in layers[1]]) == set(["Roll"])

    sch.run()

    clm_data = tbl.get_column_data('Roll')
    assert pd.isna(clm_data[0])
    assert np.isclose(clm_data[1], 8.0)
    assert np.isclose(clm_data[2], 8.0)

def test_groll_single():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="My table", attributes=["G", "A"],
        func="lambda **m: pd.DataFrame({'G': [1, 2, 1, 2], 'A': [1.0, 2.0, 3.0, 4.0]})", tables=[]
    )

    clm = sch.roll(
        name="Roll", table=tbl.id,
        window="2", link="G",
        func="lambda x: x.sum()", columns=["A"], model={}
    )

    sch.run()

    clm_data = tbl.get_column_data('Roll')

    assert pd.isna(clm_data[0])
    assert pd.isna(clm_data[1])
    assert np.isclose(clm_data[2], 4.0)
    assert np.isclose(clm_data[3], 6.0)

    pass

def test_groll_multiple():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="My table", attributes=["G", "A", "B"],
        func="lambda **m: pd.DataFrame({'G': [1, 2, 1, 2], 'A': [1, 2, 3, 4], 'B': [4, 3, 2, 1]})", tables=[]
    )

    clm = sch.roll(
        name="Roll", table=tbl.id,
        window="2", link="G",
        func="lambda x: x['A'].sum() + x['B'].sum()", columns=["A", "B"], model={}
    )

    sch.run()

    clm_data = tbl.get_column_data('Roll')

    assert pd.isna(clm_data[0])
    assert pd.isna(clm_data[1])
    assert np.isclose(clm_data[2], 10.0)
    assert np.isclose(clm_data[3], 10.0)
