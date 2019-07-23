# Prosto column-oriented data processing toolkit

Prosto is a data processing toolkit which significantly simplifies data processing and analysis. 
It radically changes the way data is processed by relying on a novel *column-oriented* data processing paradigm which treats columns as first-class elements of the data processing pipeline having the same rights as tables.

In Prosto, a workflow consists of two sorts of operations

* *Table population* operations produce new sets of tuples (tables) from existing sets. A set is a collection of values.
* *Column evaluation* operations produce new functions (columns) from existing functions. A function is a mapping of values from one set to another set.

How exactly the operations process data is specified in *user-defined functions* (in Python) which can be as simple as format conversion and as complex as deep neural networks.

# Getting started with Prosto

## Importing Prosto

Prosto is a toolkit and it is intended to be used from another (Python) application:

```python
from prosto.Workflow import *
```

## Defining a workflow

A workflow consists of a number of table and column operations. Before these operations can be added, a Prosto workflow has to be created:

```python
workflow = Workflow("My Workflow")
```

Prosto provides two types of operations which can be added to a workflow:

* Table population operation adds records to the table given records from one or more input tables
* Column evaluation operation generates values of the column given values of one or more input columns

## Defining tables

One of the simplest table operations is `population` by means of a *user-defined function* which is supposed to *know* how to populate the table:

```python
table = workflow.create_populate_table(
    "My Table", ["A", "B"],
    function="lambda x: xxx"
    )
```

Here we use a lambda function passed as a string for simplicity but it could be a reference to an arbitrary Python function. This function returns a `pandas` data frame with the table data. In more realistic cases, the function could read data from some data source or process data from input tables.

Other table operation types like `project`, `product` and `filter` allow for generating table data from already existing input tables.

## Defining columns

One of the simplest column operations is a `calculate` column which computes each its output value from the values of the input columns:

```python
column = workflow.create_calculate_column(
    "My Column", "My Table",
    function="lambda x: x['A'] + x['B']", columns=["A", "B"]
    )
```

In this example, the new column will compute the sum of columns "A" and "B".

Other column operation types like `link`, `grouping` or `rolling` allow for producing link columns referencing records in other tables and aggregate data.

## Executing a workflow

When a workflow is defined it is not executed by storing only operation definitions. In order to really process data, it is necessary to execute the workflow:

```python
workflow.run()
```

Prosto translates the workflow into a graph of operations taking into account their dependencies and then executes each operation: table operations will populate tables and column operations will evaluate columns.

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

## Install from package

Create wheel package:

```console
$ python setup.py bdist_wheel
```

The `whl` package will be stored in the `dist` folder and can then be installed using `pip`.

Execute this command by specifying the `whl` file as a parameter:

```console
$ pip install dist\prosto-0.1.0-py3-none-any.whl
```

# How to test

Run tests from the project root:

```console
$ python -m unittest discover -s ./tests
```

or

```console
$ python setup.py test
```
