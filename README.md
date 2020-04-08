```
 ____                _        
|  _ \ _ __ ___  ___| |_ ___   _______________________
| |_) | '__/ _ \/ __| __/ _ \ 
|  __/| | | (_) \__ \ || (_) | Data Processing Toolkit
|_|   |_|  \___/|___/\__\___/  _______________________
```

# What is Prosto?

`Prosto` is a data processing toolkit which significantly simplifies data processing and analysis. It radically changes the way data is processed by relying on a novel data processing paradigm which treats columns as first-class elements of the data processing pipeline having the same rights as tables. Accordingly, a `Prosto` workflow consists of two kinds of operations:

* *Table (population) operations* produce new sets of tuples (tables) from existing sets. A set is treated as a collection of values (tuples).
* *Column (evaluation) operations* produce new functions (columns) from existing functions. A function is treated as a mapping of values from one set to another set.

The data processing logic is provided via *user-defined functions* (in Python) which can be as simple as format conversion and as complex as as a machine learning algorithm.

Conceptually, it is an alternative (and opposed) to *set-oriented* approaches to data processing like map-reduce, relational algebra, SQL or data-frame-based techniques like Python pandas.

## Why Prosto?

Prosto provides the following unique features and benefits:

* *Processing data in multiple tables.* Of course, we could easily implemented calculate columns (as demonstrated in examples) using `apply` method of `pandas`. However, we cannot use this technique in the case of multiple tables. `Prosto` makes it easy to process data stored in many tables by adding new calculated columns and using links between them.

* *Getting rid of joins.* We could process data in multiple tables using the relational join operation. However, this tedious and error prone approach requires high expertise especially in the case of many tables. `Prosto` does not use joins. Instead, it relies on `link` columns which also have definitions and are part of one workflow.

* *Getting rid of group-by.* Data aggregation is typically performed using some kind of group-by operation. `Prosto` does not use this relational operation by providing column operations for that purpose which are simpler and more natural especially in describing complex analytical workflows.

* *Flexibility via user-defined functions.* `Prosto` is very flexible in defining how data will be processed because it relies on user-defined functions which are its minimal units of data processing.

# Getting started with Prosto

## Importing Prosto

`Prosto` is a toolkit and it is intended to be used from another (Python) application. Before its data processing functions can be used, the module has to be imported:

```python
import prosto as pr
```

## Defining a workflow

A workflow contains definitions of data elements (tables and columns) as well as operations for data generation. Before data processing operations can be defined, a `Prosto` workflow has to be created:

```python
prosto = pr.Prosto("My Prosto Workflow")
```

`Prosto` provides two types of operations which can be used in a workflow:

* A *table population operation* adds new records to the table given records from one or more input tables
* A *column evaluation operation* generates values of the column given values of one or more input columns

## Defining tables

Each table has some structure which is defined by its *attributes*. Table data is defined by the tuples it consists of and each tuple is a combination of some attribute values.

There exist many different ways to populate a table with tuples (attribute values). One of the simplest one is a table `population` operation. It relies on a *user-defined function* which is supposed to *know* how to populate the table by returning a `pandas` data frame with the data:

```python
sales_data = {
    "product_name": ["beer", "chips", "chips", "beer", "chips"],
    "quantity": [1, 2, 3, 2, 1],
    "price": [10.0, 5.0, 6.0, 15.0, 4.0]
}

sales = prosto.create_populate_table(
    # A table definition consists of a name and a list of attributes
    table_name="Sales", attributes=["product_name", "quantity", "price"],

    # Table operation is an UDF, list of input tables and model (parameters for UDF)
    func=lambda **m: pd.DataFrame(sales_data), tables=[], model={},

    # This parameter says that UDF returns a complete data frame
    input_length="table"
)
```

The user-defined function in this example returns a `pandas` data frame with in-memory sales data. In a more realistic case, the data could be loaded from a CSV file or database. This data frame has to contain all attributes declared for this table.

Other table operations like `project`, `product` and `filter` allow for processing table data from already existing input tables which in turn could be populated using other operations.

## Defining columns

A column is formally interpreted as a mathematical function which maps tuples (defined by table attributes) of this table to tuples in another table.

There exist many different ways to compute a mapping form one table to another table. One of the simplest column operations is a `calculate` column which *computes* output values of the mapping using the values of the specified input columns of the same table:

```python
calc_column = prosto.create_calculate_column(
    # Column definition consists of a name and table it belongs to
    name="amount", table=sales.id,

    # Column operation is UDF, list of input columns and model (parameters for UDF)
    func=lambda x: x["quantity"] * x["price"], columns=["quantity", "price"], model=None,

    # This parameter says that the UDF returns one value (not a whole column)
    input_length="value"
)
```

This new column will store the amount computed for each record as a product of quantity and price. Note that the input columns could be also derived columns computed from some other data in this or other tables.

Other column operations like `link`, `aggregate` or `rolling` allow for producing link columns referencing records in other tables and aggregate data.

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

## Matrix operations vs. set operations

It is important to understand the following crucial difference between matrixes and sets:

> A cell of a matrix is a point in the multidimensional space defined by the matrix axes - the space has as many dimensions as the matrix has axes. Values are defined for all points of the space.
> A tuple of a set is a point in the space defined by the table columns - the space has as many dimensions as the table has column. Values are defined only for a subset of all points of the space.

Obviously, this difference makes it extremely difficult to combine these two semantics in one framework.

`prosto` is an implementation of the set-oriented approach where a table represents a set and its rows represent tuples.
Note however that `prosto` supports an extended version of the set-oriented approach which includes also function as first-class elements of the model.

## `pandas` vs. `prosto`

`pandas` is very powerful toolkit which relies on the notion of matrix for data representation. In other words, a matrix is the main unit of data representation in `pandas`.
Yet, `pandas` supports not only matrix operations (in this case, having `numpy` would be enough) but also set operations and relational operations as well as map-reduce and OLAP and some other conceptions. In this sense, `pandas` is a quite eclectic toolkit. 

In contrast, `prosto` is based on only one theoretical basis: the concept-oriented model of data. For simplicity, it can be viewed as a purely set-oriented model (not the relational model) along with a function-oriented model.
Yet, `prosto` relies on `pandas` in its implementation just because `pandas` provides a really powerful set of various highly optimized operations with data. Yet, these operations are used as one possible iplementation method by essentially changing their semantics when wrapped into `prosto` operations.

## Sets and functions

A *set* is a collection of *tuples*. A set is a formal representation of a collection of values. Tuples (data values) can be only added to or removed from a set. In `prosto`, we refer to sets as tables, that is, tables implement sets. 

A tuple has structure declared by its *attributes*. Tuples are a formal representation of data values.

A *function* is a mapping from an input set to an output set. Given an input value, the output value can be read from the function or set for the function. In `prosto`, we refer to functions as columns, that is, columns are implementations of functions.

## Operations

### Calculate columns (instead of map operation)

Probably the simplest and most frequent operation in `prosto` is computing a new column of the table which is done by defining a `calculate` column. The main computational part of the definition is a (Python) function which returns a single value computed from one or more input values in its arguments. 
```
TODO: Examples of calculate function
```

This function will be evaluated for each row of the table and its outputs will be stored as a new column. 

It is precisely how `apply` works in `pandas` (and actually it relies on it in its implementation) but it is different from how `map` operation works because a calculated column does not add any new table while `map` computes a new collection (which makes computations less efficient). 

The `prosto` approach is somewhat similar to spreadsheets with the difference that new columns depend on only one coordinate - other columns - while cells in spreadsheets depend on two coordinates - row and column addresses. The both however are equally simple and natural.   

```
TODO: Examples of new cell definition and new column definition usign pseudo-code
```

### Link columns (instead of join)

We can define and evaluate new columns only in individual tables but we cannot define a new column which depends on the data in another table. Link columns solve this problem. A link column stores values which uniquely represent rows of a target (linked) table. In this sense, it is a normal column with some values which are computed using some definition. The difference is how these values are computed and their semantics. They do not have a domain-specific semantics but rather they are understood only by the system. More specificially, each value of a link column is a reference to a row in the linked table or None in the case it does not reference anything. 

The main part of the definition is a criterion for finding a target row which matching this row. The most most wide spread criterion is based on equality of some values in two rows and the definition includes lists of the columns which have to be equal in order for this row to reference the target row.
```
TODO: Examples of calculate function
```

Link columns have several major uses:
* Data in other (linked) tables can be accessed when doing something in this table, say, when defining its calculate columns
* Data can be grouped using linked rows interpreted as groups, that is, all rows of this table referencing the same row of the target table are interpreted as one group 
* Link columns are used when defining aggregate columns

There could be other criteria for matching rows and defining link columns which will be implemented in future versions.

### Merge columns (instead of join)

Once we have defined link columns and interlinked our (initially isolated) set of tables, the question is how we can use these links?
Currently, the only way is to move data between table by copying linked columns performed by the merge operation. It copies a column from the target linked table into this table. In this sense, it simply copies data between tables.
Its definition is very simple: we need to specify only the link column and the target column. 

The copied (merged) columns can be then used in other operations like calculate columns or aggregate columns.   

Note that the merge operation (as an explicit operation) is planned to become obsolete in future versions (but can still be used behind the scenes). Yet, currently it is the only way to access data in other tables using link columns. 

### Rolling columns (instead of over-partition)

TBD

### Aggregate columns (instead of groupby)

TBD

### Filtering tables (instead of select-where)

It is one of the most frequently used operations. The main difference form conventional implementations is that the result never includes the source table columns. Instead, the result (filtered) table references the selected source rows using an automatically created link column.
If it is necessary to use the source table data (and it is almost always the case) then they are accessible via the created link column. 

### Projecting tables (instead of select-distinct)

This operation has these important uses:
* Creating a table with group elements for aggregation because (in contrast to other approaches) it must exist
* Creating a dimension table for multi-dimensional analysis in the case it does not exist

### Product of tables (instead of join)

Uses:
* Creating a cube table from dimension tables for multi-dimensional analysis. It is typically followed by aggregating data in the fact table. 

# Reference

## Column operations

* create_calculate_column
* create_link_column
* create_merge_column
* create_rolling_column
* create_aggregate_column

## Table operations

* create_populate_table
* create_product_table
* create_filter_table
* create_project_table

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
