# Quick start

## Importing Prosto

`Prosto` is a toolkit and it is intended to be used from another (Python) application. Before its data processing functions can be used, the module has to be imported:

```python
import prosto as pr
```

## Defining a workflow

A workflow contains *definitions* of data elements (tables and columns) as well as *operations* for data generation. Before data processing operations can be defined, a `Prosto` workflow has to be created:

```python
prosto = pr.Prosto("My Prosto Workflow")
```

`Prosto` provides two types of operations which can be used in a workflow:

* A *table population operation* adds new records to the table given records from one or more input tables
* A *column evaluation operation* generates values of the column given values of one or more input columns

## Defining a table

Each table has some structure which is defined by its *attributes*. Table data is defined by the tuples it consists of and each tuple is a combination of some attribute values.

There exist many different ways to populate a table with tuples (attribute values). One of the simplest one is a table `populate` operation. It relies on a *user-defined function* which is supposed to "know" how to populate this table by returning a `pandas` data frame with the data.

Below we define a table with three attributes which will be populated by the specified user-defined function:

```python
sales_data = {
    "product_name": ["beer", "chips", "chips", "beer", "chips"],
    "quantity": [1, 2, 3, 2, 1],
    "price": [10.0, 5.0, 6.0, 15.0, 4.0]
}

sales = prosto.populate(
    # Table definition consists of a name and list of attributes
    table_name="Sales", attributes=["product_name", "quantity", "price"],

    # Table operation is an UDF, list of input tables and model (parameters for UDF)
    func=lambda **m: pd.DataFrame(sales_data), tables=[], model={}
)
```

The user-defined function in this example returns a `pandas` data frame with in-memory sales data. In a more realistic case, the data could be loaded from a CSV file or database. This data frame has to contain all attributes declared for this table.

Other table operations like `project`, `product` and `filter` allow for processing table data from already existing input tables which in turn could be populated using other operations.

## Defining a column

A column is formally interpreted as a mathematical function which maps tuples (defined by table attributes) of this table to tuples in another table.

There exist many different ways to compute a mapping form one table to another table. One of the simplest column operations is a `calculate` column which *computes* output values of the mapping using the values of the specified input columns of the same table:

```python
calc_column = prosto.calculate(
    # Column definition consists of a name and table it belongs to
    name="amount", table=sales.id,

    # Column operation is UDF, list of input columns and model (parameters for UDF)
    func=lambda x: x["quantity"] * x["price"], columns=["quantity", "price"], model=None
)
```

This new column will store the amount computed for each record as a product of quantity and price. Note that the input columns could be also derived columns computed from some other data in this or other tables.

Other column operations like `link`, `aggregate` or `roll` allow for producing link columns referencing records in other tables and aggregate data.

## Executing a workflow

When a workflow is defined it is not executed - it stores only operation definitions. In order to really process data, the workflow has to be executed:

```python
prosto.run()
```

`Prosto` translates a workflow into a graph of operations (topology) taking into account their dependencies and then executes each operation: table operations will populate tables and column operations will evaluate columns.

Now we can explore the result by reading data form the table along with the calculate column:

```python
df = table.get_data()
print(df)
```

```
   product_name quantity price amount
0  beer         1        10.0  10.0
1  chips        2        5.0   10.0
2  chips        3        6.0   18.0
3  beer         2        15.0  30.0
4  chips        1        4.0   4.0
```

Although it looks like a normal table, the last column was derived from the data in other columns. In more complex cases, column data and table data will be derived from columns in other tables.
