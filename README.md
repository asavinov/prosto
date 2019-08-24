<img src="https://github.com/prostodata/prostopy/raw/master/docs/images/ProstoLogo.png" height="64" width="64" align="absmiddle"> Prosto Data
=============================================================================================================================================

# Prosto data processing toolkit

`Prosto` is a data processing toolkit which significantly simplifies data processing and analysis. It radically changes the way data is processed by relying on a novel data processing paradigm which treats columns as first-class elements of the data processing pipeline having the same rights as tables. Accordingly, a `Prosto` workflow consists of two kinds of operations:

* *Table (population) operations* produce new sets of tuples (tables) from existing sets. A set is treated as a collection of values (tuples).
* *Column (evaluation) operations* produce new functions (columns) from existing functions. A function is treated as a mapping of values from one set to another set.

How exactly the operations process data is normally specified via a *user-defined functions* (in Python) which can be as simple as format conversion and as complex as as a machine learning algorithm.

# Getting started with Prosto

## Importing Prosto

`Prosto` is a toolkit and it is intended to be used from another (Python) application. Before its data processing function can be used, the module has to be imported:

```python
import prosto as prst
```

## Defining a workflow

A workflow contains definitions of data elements (tables and columns) as well as operations for data generation. Before data processing operations can be defined, a `Prosto` workflow has to be created:

```python
workflow = prst.Workflow("My Workflow")
```

`Prosto` provides two types of operations which can be used in a workflow:

* A *table population operation* adds new records to the table given records from one or more input tables
* A *column evaluation operation* generates values of the column given values of one or more input columns

## Defining tables

Each table has some structure which is defined by its *attributes*. Table data is defined by the tuple it consists of and each tuple is a combination of some attribute values.

There exist many different ways to populate a table with tuples (attribute values). One of the simplest one is a table `population` operation. It relies on a *user-defined function* which is supposed to *know* how to populate the table by returning a `pandas` data frame with the data:

```python
sales_data = {
    'product_name': ["beer", "chips", "chips", "beer", "chips"],
    'quantity': [1, 2, 3, 2, 1],
    'price': [10.0, 5.0, 6.0, 15.0, 4.0]
}

sales = workflow.create_populate_table(
    # A table definition consists of a name and a list of attributes
    table_name="Sales", attributes=["product_name", "quantity", "price"],

    # Table operation is an UDF, list of input tables and model (parameters for UDF)
    func=lambda **m: pd.DataFrame(sales_data), tables=[], model={},

    # This parameter says that UDF returns a complete data frame
    input_length='table'
)
```

The user-defined function in this example returns a `pandas` data frame with in-memory sales data. In the general case, the data could be loaded from a CSV file or database. This data frame has to contain all attributes declared for this table.

Other table operations like `project`, `product` and `filter` allow for processing table data from already existing input tables which in turn could be populated using other operations.

## Defining columns

A column is formally interpreted as a mathematical function which maps tuples (defined by table attributes) of this table to tuples in another table.

There exist many different ways to compute a mapping form one table to another table. One of the simplest column operations is a `calculate` column which *computes* output values of the mapping from the values of the specified input columns of the same table:

```python
calc_column = workflow.create_calculate_column(
    # Column definition consists of a name and table it belongs to
    name="amount", table=sales.id,

    # Column operation is UDF, list of input columns and model (parameters for UDF)
    func=lambda x: x['quantity'] * x['price'], columns=["quantity", "price"], model=None,

    # This parameter says that the UDF returns one value (not a whole column)
    input_length='value'
)
```

This new column will store the amount computed for each record as a product of quantity and price. Note that the input columns could be also derived columns computed from some other data in this or other tables.

Other column operations like `link`, `grouping` or `rolling` allow for producing link columns referencing records in other tables and aggregate data.

## Executing a workflow

When a workflow is defined it is not executed - it stores only operation definitions. In order to really process data, the workflow has to be executed:

```python
workflow.run()
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

## Benefits

* Processing data in multiple tables. Of course, we could easily implemented a calculate column (as shown in the example) using `apply` method of `pandas`. However, we cannot use this technique in the case of multiple tables. `Prosto` makes it easy to process data stored in many tables.

* Getting rid of joins. We could process data in multiple tables using relational join. However, it tedious and error prone approach requiring high expertise especially in the case of many tables. `Prosto` does not use joins. Instead, it relies on `link` columns which also have definitions and are part of one workflow.

* Getting rid of group-by. Data aggregation is typically performed using some kind of group-by operation. `Prosto` does not use this relational operation by providing column operations for that purpose which are simpler and more natural especially describing complex analytical workflows.

# How to install

## Install from source code

Check out the source code and execute this command in the project directory (where `setup.py` is located):

```console
$ pip install .
```

Or alternatively:

```console
$ python setup.py install
```

## Install from repository

Install from repository:

```console
$ pip install prosto
```

This command will install the latest version of `Prosto` from repository.

# How to test

Run tests from the project root:

```console
$ python -m unittest discover -s ./tests
```

or

```console
$ python setup.py test
```
