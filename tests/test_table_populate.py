import pytest

from prosto.Prosto import *


def test_populate():
    sch = Prosto("My Prosto")

    tbl = sch.populate(
        table_name="My table", attributes=["A", "B"],
        func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0], 'B': ['x', 'y', 'z']})", tables=[], model={"nrows": 3}
    )

    tbl.evaluate()

    assert len(tbl.get_df().columns) == 2
    assert len(tbl.get_df()) == 3


def test_populate2():
    sch = Prosto("My Prosto")

    data = {'A': [1.0, 2.0, 3.0], 'B': ['x', 'y', 'z']}

    populate_fn = lambda **m: pd.DataFrame(data)

    tbl = sch.populate(
        table_name="My table", attributes=["A", "B"],
        func=populate_fn, tables=[], model={"nrows": 3}
    )

    tbl.evaluate()

    assert len(tbl.get_df().columns) == 2
    assert len(tbl.get_df()) == 3
