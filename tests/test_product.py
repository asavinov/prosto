import pytest

from prosto.Prosto import *
from prosto.column_sql import *


def test_product():
    ctx = Prosto("My Prosto")

    t1 = ctx.populate(
        table_name="Table 1", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0]})", tables=[]
    )

    t2 = ctx.populate(
        table_name="Table 2", attributes=["B"],
        func="lambda **m: pd.DataFrame({'B': ['x', 'y', 'z']})", tables=[]
    )

    product = ctx.product(
        table_name="Product", attributes=["t1", "t2"],
        tables=["Table 1", "Table 2"]
    )

    t1.evaluate()
    t2.evaluate()
    product.evaluate()

    assert len(product.get_df().columns) == 2
    assert len(product.get_df()) == 9

    assert product.get_df().columns.to_list() == ["t1", "t2"]


def test_product_inheritance():
    """
    We add an addition calculate column to the product table which uses a column of a base table.
    The system has to automatically insert a new operation by resolving this missing column.
    """
    ctx = Prosto("My Prosto")

    t1 = ctx.populate(
        table_name="Table 1", attributes=["A"],
        func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0]})", tables=[]
    )

    t2 = ctx.populate(
        table_name="Table 2", attributes=["B"],
        func="lambda **m: pd.DataFrame({'B': ['x', 'y', 'z']})", tables=[]
    )

    product = ctx.product(
        table_name="Product", attributes=["t1", "t2"],
        tables=["Table 1", "Table 2"]
    )

    # In this calculate column, we use a column of the product table which actually exists only in a base table
    clm = ctx.calculate(
        name="My column", table=product.id,
        func="lambda x: x + 1.0", columns=["A"], model=None
    )

    ctx.run()

    # We get two columns in addition to two attributes: one merge (augmented) and one calculate column
    assert len(product.get_df().columns) == 4

    clm_data = product.get_series('My column')

    assert clm_data.to_list() == [2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0, 4.0, 4.0]


def test_product_csql():
    ctx = Prosto("My Prosto")

    t1_df = pd.DataFrame({'A': [1.0, 2.0, 3.0]})
    t2_df = pd.DataFrame({'B': ['x', 'y', 'z']})

    ctx.column_sql("TABLE  Table_1 (A)", lambda **m: t1_df)
    ctx.column_sql("TABLE  Table_2 (B)", lambda **m: t2_df)
    ctx.column_sql("PRODUCT  Table_1; Table_2 -> t1; t2 -> Product")

    assert ctx.get_table("Product")

    ctx.run()

    product = ctx.get_table("Product")

    assert len(product.get_df().columns) == 2
    assert len(product.get_df()) == 9

    assert product.get_df().columns.to_list() == ["t1", "t2"]
