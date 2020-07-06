```
 ____                _        
|  _ \ _ __ ___  ___| |_ ___   ___________________________________________
| |_) | '__/ _ \/ __| __/ _ \ 
|  __/| | | (_) \__ \ || (_) | Data Processing Toolkit - noSql-noMapReduce
|_|   |_|  \___/|___/\__\___/  ___________________________________________
```
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://github.com/prostodata/prosto/blob/master/LICENSE)
[![Python 3.6](https://img.shields.io/badge/python-3.6-brightgreen.svg)](https://www.python.org/downloads/release/python-370/)
[![PyPI version](https://badge.fury.io/py/prosto.svg)](https://badge.fury.io/py/prosto)

* [What is Prosto?](#what-is-prosto)
* [Why Prosto?](#why-prosto)
* [Getting started with Prosto](#getting-started-with-prosto)
* [Concepts](#concepts)
* [Prosto operations](#prosto-operations)
* [How to use](#how-to-use)

# What is Prosto?

`Prosto` is a data processing toolkit which significantly simplifies data processing and analysis. 

Conceptually, it is an alternative to *set-oriented* approaches to data processing like map-reduce, relational algebra, SQL or data-frame-based tools like Python `pandas`.

`Prosto` radically changes the way data is processed by relying on a novel data processing paradigm which treats columns (mathematical functions) as first-class elements of the data processing pipeline having the same rights as tables. Accordingly, a `Prosto` workflow consists of two categories of operations:

* *Table operations* produce (populate) new tables from existing tables. A table is an implementation of a mathematical *set* which is a collection of tuples.
* *Column operations* produce (evaluate) new columns from existing columns. A column is an implementation of a mathematical *function* which is a mapping of values from one set to another set.

# Why Prosto?

Prosto provides the following unique features and benefits:

* *Processing data in multiple tables.* We can easily implement calculate columns (as demonstrated in examples) using `apply` method of `pandas`. However, we cannot use this technique in the case of multiple tables. `Prosto` is intended for and makes it easy to process data stored in many tables by relying on `link` columns which are also evaluated from the data.

* *Getting rid of joins.* Data in multiple tables can be processed using the relational join operation. However, it is tedious, error prone and requires high expertise especially in the case of many tables. `Prosto` does not use joins. Instead, it relies on `link` columns which also have definitions and are part of one workflow.

* *Getting rid of group-by.* Data aggregation is typically performed using some kind of group-by operation. `Prosto` does not use this relational operation by providing column operations for that purpose which are simpler and more natural especially in describing complex analytical workflows.

* *Flexibility via user-defined functions.* `Prosto` is very flexible in defining how data will be processed because it relies on user-defined functions which are its minimal units of data processing. They provide the logic of processing at the level of individual values while the logic of looping through the sets is implemented by the system according to the type of operation applied. User-defined functions can be as simple as format conversion and as complex as as a machine learning algorithm.

* Data Dictionary (DD) for declaring schema, tables and columns, and Feature Store (FS) functions for definition operations over these data objects

* In future, `Prosto` will implement such features as *incremental evaluation* for processing only what has changed, *model training* for training models as part of the workflow, data/model persistence and other data processing and analysis operations.

# Getting started with Prosto

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

There exist many different ways to populate a table with tuples (attribute values). One of the simplest one is a table `populate` operation. It relies on a *user-defined function* which is supposed to "know" how to populate this table by returning a `pandas` data frame with the data:

```python
sales_data = {
    "product_name": ["beer", "chips", "chips", "beer", "chips"],
    "quantity": [1, 2, 3, 2, 1],
    "price": [10.0, 5.0, 6.0, 15.0, 4.0]
}

sales = prosto.populate(
    # Table definition consists of a name and a list of attributes
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

Although it looks like a normal table, the last column was derived from the data in other columns. In more realistic cases, column data and table data will be derived from columns in other tables.

# Concepts

## Matrixes vs. sets

It is important to understand the following crucial difference between matrixes and sets expressed in terms of multidimensional spaces:

> A cell of a matrix is a point in the multidimensional space defined by the matrix axes - the space has as many dimensions as the matrix has axes. Values are defined for all points of the space.
> A tuple of a set is a point in the space defined by the table columns - the space has as many dimensions as the table has column. Values are defined only for a subset of all points of the space.

It is summarized in the table:

| Property          | Matrix                | Set                    |
| ---               | ---                   | ---                    |
| Dimension         | Axis                  | Attribute              |
| Point coordinates | Cell axes values      | Tuple attribute values |
| Dimensionality    | Number of axes        | Number of attributes   |
| Represents        | Distribution          | Predicate              |
| Point             | Value of distribution | True of false          |

The both structure can represent some distribution over a multidimensional space but do it in different ways. Obviously, these differences make it extremely difficult to combine these two semantics in one framework.

`Prosto` is an implementation of the set-oriented approach where a table represents a set and its rows represent tuples. Note however that `Prosto` supports an extended version of the set-oriented approach which includes also functions as first-class elements of the model.

## Sets vs. functions

*Tuples* are a formal representation of data values. A tuple has structure declared by its *attributes*.

A *set* is a collection of *tuples*. A set is a formal representation of a collection of values. Tuples (data values) can be only added to or removed from a set. In `Prosto`, sets are implemented via table objects. 

A *function* is a mapping from an input set to an output set. Given an input value, the output value can be read from the function or set for the function. In `Prosto`, functions are implemented via column objects.

## Attributes vs. columns

Attributes define the structure of tuples and they are not evaluated. Attribute values are set by the table population procedure.

Columns implement functions (mappings between sets) and their values are computed by the column evaluation procedure.

## `Pandas` vs. `Prosto`

`Pandas` is a very powerful toolkit which relies on the notion of matrix for data representation. In other words, matrix is the main unit of data representation in `pandas`. Yet, `pandas` supports not only matrix operations (in this case, having `numpy` would be enough) but also set operations, relational operations, map-reduce, multidimensional and OLAP as well as some other conceptions. In this sense, `pandas` is a quite eclectic toolkit. 

In contrast, `Prosto` is based on only one theoretical basis: the concept-oriented model of data. For simplicity, it can be viewed as a purely set-oriented model (not the relational model) along with a function-oriented model. Yet, `Prosto` relies on `pandas` in its implementation just because `pandas` provides a powerful set of various highly optimized operations with data.

# Prosto operations

## List of operations

`Prosto` currently supports the following operations:

* Column operations

  * `compute`: A complete new column is computed from the input columns of the same table. It is analogous to table `populate` operation
  * `calculate`: New column values are computed from other values in the same table and row
  * `link`: New column values uniquely represent rows from another table
  * `merge`: New columns values are copied from a linked column in another table
  * `roll`: New column values are computed from the subset of rows in the same table
  * `aggregate`: New column values are computed from a subset of row in another table
  * `discretize`: New column values are a finite number of groups like numeric intervals

* Table operations

  * `populate`: A complete table with all its rows is populated and returned by the specified UDF
  * `product`: A new table consists of all combinations of rows in the inputs tables
  * `filter`: A new table is a subset of rows from another table selected using the specified UDF
  * `project`: A new table consists of all unique combinations of the specified columns of the input table

Examples of these operations can be found in unit tests or Jupyter notebooks in the `notebooks` project folder.

## Column operations

### Calculate column (instead of map operation)

Probably the simplest and most frequent operation in `Prosto` is computing a new column of the table which is done by defining a `calculate` column. The main computational part of the definition is a (Python) function which returns a single value computed from one or more input values in its arguments. 

This function will be evaluated for each row of the table and its outputs will be stored as a new column. 

It is precisely how `apply` works in `pandas` (and actually it relies on it in its implementation) but it is different from how `map` operation works because a calculated column does not add any new table while `map` computes a new collection (which makes computations less efficient). 

The `Prosto` approach is somewhat similar to spreadsheets with the difference that new columns depend on only one coordinate - other columns - while cells in spreadsheets depend on two coordinates - row and column addresses. The both however are equally simple and natural.   

Check out the `calculate.ipynb` notebook for a working example of the `calculate` operaiton.

### Link column (instead of join)

We can define and evaluate new columns only in individual tables but we cannot define a new column which depends on the data in another table. Link columns solve this problem. A link column stores values which uniquely represent rows of a target (linked) table. In this sense, it is a normal column with some values which are computed using some definition. The difference is how these values are computed and their semantics. They do not have a domain-specific semantics but rather they are understood only by the system. More specifically, each value of a link column is a reference to a row in the linked table or None in the case it does not reference anything. 

The main part of the definition is a criterion for finding a target row which matching this row. The most wide spread criterion is based on equality of some values in two rows and the definition includes lists of the columns which have to be equal in order for this row to reference the target row.

Link columns have several major uses:
* Data in other (linked) tables can be accessed when doing something in this table, say, when defining its calculate columns
* Data can be grouped using linked rows interpreted as groups, that is, all rows of this table referencing the same row of the target table are interpreted as one group 
* Link columns are used when defining aggregate columns

There could be other criteria for matching rows and defining link columns which will be implemented in future versions.

Check out the `link.ipynb` notebook for a working example of the `link` operaiton.

### Merge column (instead of join)

Once we have defined link columns and interlinked our (initially isolated) set of tables, the question is how we can use these links? Currently, the only way is to move data between table by copying linked columns performed by the merge operation. It copies a column from the target linked table into this table. In this sense, it simply copies data between tables. Its definition is very simple: we need to specify only the link column and the target column. 

The copied (merged) columns can be then used in other operations like calculate columns or aggregate columns.   

Note that the merge operation (as an explicit operation) is planned to become obsolete in future versions (but can still be used behind the scenes). Yet, currently it is the only way to access data in other tables using link columns. 

### Rolling aggregation column (instead of over-partition)

This column will aggregate data located in "neighbor" rows of this same table which are selected using criteria in the window objects. For example, we can specify how many previous rows to select. 

Currently, its logic is equivalent to that of the rolling aggregation in `pandas` with the difference that the result column is immediately added to the table and this operation is part of the whole workflow.

Check out the `roll.ipynb` notebook for a working example of rolling aggregation.

### Aggregate column (instead of groupby)

This column aggregates data in groups of rows selected from another table. The selection is performed by specifying an existing link column which links the fact table with this (group) table. The new column is added to this (group) table. 

Currently, its logic is equivalent to that of the groupby in `pandas` with the difference that the result column is added to the existing table and the two tables must be linked beforehand.

Check out the `aggregate.ipynb` notebook for a working example of aggregation.

### Discretize column

Let us assume that we have a numeric column but we want to partition it into a finite number of intervals and then use these intervals intead of numeric values. The `discretize` coumn produces a new column with a finite number of values where each such value represents a group the input value belongs to.

How the groups are identified and how the input space is partitioned is defined in the model. In the simplest case, there is one numeric column and the model defines intervals with equal length. These intervals are identified by their border value (left or right). The output columm will contain border values for the intervals input values belong to. For example, if we have temperature values in the input column like 21.1, 23.3, 22.2 etc. but we want to use discrete values like 21, 23, 22, then we need to define a `discretize` column. In this case, it is similar to rounding (which can be implemented using a `calculate` column) but the logic of discretization can be more complicated.

Links:
* https://numpy.org/doc/stable/reference/generated/numpy.digitize.html

## Table operations

### Filter table (instead of select-where)

It is one of the most frequently used operations. The main difference form conventional implementations is that the result never includes the source table columns. Instead, the result (filtered) table references the selected source rows using an automatically created link column. If it is necessary to use the source table data (and it is almost always the case) then they are accessible via the created link column. 

### Project table (instead of select-distinct)

This operation has these important uses:
* Creating a table with group elements for aggregation because (in contrast to other approaches) it must exist
* Creating a dimension table for multi-dimensional analysis in the case it does not exist

Check out the `project.ipynb` notebook for a working example of the `project` operaiton.

### Product of tables (instead of join)

This table is intended to produce all combinations of rows in other tables. Its main difference from the relational model is that the result table stores links to the rows of the source tables rather than copies of its rows. The result table has as many attributes as it has source tables in its definition. (In contrast, the number of attributes in a relational product is equal to the sum of attributes in all source tables.)

Uses:
* Creating a cube table from dimension tables for multi-dimensional analysis. It is typically followed by aggregating data in the fact table. 

### Range table

This operation populates a table with one attribute which contains values from a range described in the model. A range specification typically has such parameters as `start`, `end`, `step` size (or frequency), `origin` and others depending on the range type.

Links:
* https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.date_range.html
* https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#generating-ranges-of-timestamps

# How to use

## Install from source code

Check out the source code and execute this command in the project directory (where `setup.py` is located):

```console
$ pip install .
```

Or alternatively:

```console
$ python setup.py install
```

## Install from PYPI

This command will install the latest release of `Prosto` from PYPI:

```console
$ pip install prosto
```

## How to test

Run tests from the project root:

```console
$ python -m unittest discover -s ./tests
```

or

```console
$ python setup.py test
```
