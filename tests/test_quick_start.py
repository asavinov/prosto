import pytest

import pandas as pd

import prosto as pr


def test_quick_start():
    prosto = pr.Prosto("My Prosto Workflow")

    #
    # First (facts) table
    #
    items_data = {
        'name': ["beer", "chips", "chips", "beer", "chips"],
        'quantity': [1, 2, 3, 2, 1],
    }
    items_df = pd.DataFrame(data=items_data)

    items = prosto.populate(
        # A table definition consists of a name and a list of attributes
        table_name="Items", attributes=["name", "quantity"],

        # Table operation is UDF, input tables and model
        func=lambda **m: items_df, tables=[]
    )

    #
    # Second (group) table
    #
    products_data = {
        'name': ["beer", "chips", "tee"],
        'price': [10.0, 4.0, 2],
    }
    products_df = pd.DataFrame(data=products_data)

    products = prosto.populate(
        # A table definition consists of a name and a list of attributes
        table_name="Products", attributes=["name", "price"],

        # Table operation is UDF, input tables and model
        func=lambda **m: products_df, tables=[]
    )

    #
    # Link column
    #
    link_column = prosto.link(
        # In contrast to other columns, a link column specifies its target table name
        name="product", table=items.id, type=products.id,

        # It is a criterion of linking: all input columns have to be equal to the output columns
        columns=["name"], linked_columns=["name"]
    )

    #
    # Calc column
    #
    calc_column = prosto.calculate(
        # Column definition consists of a name and a table it belongs to
        name="amount", table=items.id,

        # Column operation is UDF, input columns and model
        func=lambda x: x["quantity"] * x["product::price"], columns=["quantity", "product::price"]
    )

    #
    # Aggregate column
    #
    total = prosto.aggregate(
        # Column description
        name="total", table=products.id,
        # How to group
        tables=["Items"], link="product",
        # How to aggregate
        func="lambda x: x.sum()", columns=["amount"], model={}
    )

    #
    # Execute workflow
    #
    prosto.run()

    total = products.get_series("total")

    assert total[0] == 30.0
    assert total[1] == 24.0
    assert total[2] == 0.0
