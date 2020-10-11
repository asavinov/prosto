import pytest

from prosto.Prosto import *

def test_integers():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="My table", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9]})", tables=[]
    )

    clm = sch.discretize(
        name="My column", table=tbl.id,
        columns=["A"], model={"origin": 5, "step": 3}
    )
    #  1, [2, 3, 4, [5, 6, 7, [8, 9
    # -2  -1         0         1

    sch.run()

    clm_data = tbl.get_column_data('My column')
    assert list(clm_data) == [-2, -1, -1, -1, 0, 0, 0, 1, 1]

def test_integers2():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="My table", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9]})", tables=[]
    )

    clm = sch.discretize(
        name="My column", table=tbl.id,
        columns=["A"], model={"origin": 5, "step": 3, "label": "right", "closed": "right"}
    )
    #  1, 2], 3, 4, 5], 6, 7, 8], 9
    # -1 -1         0         1   2

    sch.run()

    clm_data = tbl.get_column_data('My column')
    assert list(clm_data) == [-1, -1, 0, 0, 0, 1, 1, 1, 2]
