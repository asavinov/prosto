import pytest

from prosto.Prosto import *


def test_integers():
    ctx = Prosto("My Prosto")

    tbl = ctx.populate(
        table_name="My table", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9]})", tables=[]
    )

    clm = ctx.discretize(
        name="My column", table=tbl.id,
        columns=["A"], model={"origin": 5, "step": 3}
    )
    #  1, [2, 3, 4, [5, 6, 7, [8, 9
    # -2  -1         0         1

    ctx.run()

    clm_data = tbl.get_series('My column')
    assert list(clm_data) == [-2, -1, -1, -1, 0, 0, 0, 1, 1]


def test_integers2():
    ctx = Prosto("My Prosto")

    tbl = ctx.populate(
        table_name="My table", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9]})", tables=[]
    )

    clm = ctx.discretize(
        name="My column", table=tbl.id,
        columns=["A"], model={"origin": 5, "step": 3, "label": "right", "closed": "right"}
    )
    #  1, 2], 3, 4, 5], 6, 7, 8], 9
    # -1 -1         0         1   2

    ctx.run()

    clm_data = tbl.get_series('My column')
    assert list(clm_data) == [-1, -1, 0, 0, 0, 1, 1, 1, 2]
