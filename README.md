<img src="https://github.com/prostodata/prostopy/raw/master/docs/images/ProstoLogo.png" height="64" width="64" align="absmiddle"> Prosto Data
=============================================================================================================================================

# Prosto data processing toolkit

`Prosto` is a data processing toolkit which significantly simplifies data processing and analysis. 
It radically changes the way data is processed by relying on a novel *column-oriented* data processing paradigm which treats columns as first-class elements of the data processing pipeline having the same rights as tables.

In `Prosto`, a workflow consists of two categories of operations

* *Table population* operations produce new sets of tuples (tables) from existing sets. A set is a collection of values.
* *Column evaluation* operations produce new functions (columns) from existing functions. A function is a mapping of values from one set to another set.

How exactly the operations process data is specified in *user-defined functions* (in Python) which can be as simple as format conversion and as complex as as a machine learning model like deep neural network.

# Getting started with Prosto

## Importing Prosto

`Prosto` is a toolkit and it is intended to be used from another (Python) application. Before its function can be used, the module has to be imported:

```python
import prosto as prst
```

## Defining a workflow

A workflow consists of a number of table and column operations. Before these operations can be added, a `Prosto` workflow has to be created:

```python
workflow = prst.Workflow("My Workflow")
```

`Prosto` provides two categories of operations which can be added to a workflow:

* A *table population* operation adds new records to the table given records from one or more input tables
* A *column evaluation* operation generates values of the column given values of one or more input columns

## Defining tables

Each table has some structure which is defined by its *attributes*. Table attributes define the structure of tuple this table consists of as a set. (In contrast, table columns are thought of as functions which map tuples of this set to tuples of another set.)

One of the simplest table operations is `population` by means of a *user-defined function* which is supposed to *know* how to populate the table:

```python
table = workflow.create_populate_table(
    name="My Table", attributes=["A", "B"],
    function="lambda x: xxx"
    )
```

Here we use a lambda function passed as a string for simplicity but it could be a reference to an arbitrary other Python function. This function returns a `pandas` data frame with the table data. This data frame has to contain all attributes declared for this table (`A` and `B` in this example). In more realistic cases, the function could read data from some data source or process data from input tables.

Other table operation like `project`, `product` and `filter` allow for processing table data from already existing input tables.

## Defining columns

A column is interpreted formally as a function which maps tuples (defined by the table attributes) of this table to tuples of another table.

One of the simplest column operations is a `calculate` column which *computes* output values of the mapping from the values of the input columns of the same table:

```python
column = workflow.create_calculate_column(
    name="My Column", table_name="My Table",
    function="lambda x: x['A'] + x['B']", columns=["A", "B"]
    )
```

In this example, the new column will compute the sum of columns `A` and `B`.

Other column operations like `link`, `grouping` or `rolling` allow for producing link columns referencing records in other tables and aggregate data.

## Executing a workflow

When a workflow is defined it is not executed - it stores only operation definitions. In order to really process data, it has to be executed:

```python
workflow.run()
```

`Prosto` translates the workflow into a graph of operations (topology) taking into account their dependencies and then executes each operation: table operations will populate tables and column operations will evaluate columns.

Now we can explore the result:

```python
df = table.get_data()
print(df)
series = column.get_data()
print(series)
```

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
