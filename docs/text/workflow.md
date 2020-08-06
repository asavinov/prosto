# Workflow and operations

## Structure of workflow

TBD

## List of operations

`Prosto` currently supports the following operations:

* Column operations

  * `compute`: A complete new column is computed from the input columns of the same table. It is analogous to the table `populate` operation
  * `calculate`: New column values are computed from other values in the same table and row
  * `link`: New column values uniquely represent rows from another table
  * `merge`: New columns values are copied from a linked column in another table
  * `roll`: New column values are computed from the subset of rows in the same table
  * `aggregate`: New column values are computed from a subset of row in another table
  * `discretize`: New column values are a finite number of groups like numeric intervals

* Table operations

  * `populate`: A complete table with all its rows is populated and returned by the specified UDF similar to the column `compute` operaiton
  * `product`: A new table consists of all combinations of rows in the inputs tables
  * `filter`: A new table is a subset of rows from another table selected using the specified UDF
  * `project`: A new table consists of all unique combinations of the specified columns of the input table

Examples of these operations can be found in unit tests or Jupyter notebooks in the `notebooks` project folder.

## Operation parameters

An operation in Prosto provides a general logic of data processing and it does not do anything by itself. An operation needs additional parameters which specify what exactly has to be done with the data. Below we describe parameters which are common to almost all operation types.

* Data elements and operations. It is important to understand that data elements and operations are different types of objects and they are managed separately in `Prosto`. We can create, update and delete them separately. Yet, for simplicity, Prosto provides functions which create an operation along with the corresponding new data element. For example, we call the `calculate` function then it will define one column and one operation. A new data element and a new operation are described by different parameters of the function.

* Data element definition. First two parameters of an operation define a data element. If it is a column operation like `link` then it defines a new column using its `name` and (existing) `table`. If it is a table operation like `project` then it is its `table_name` and a list of `attributes`. The rest of the operation parameters define an operation.

* Function. Most operations have a `func` argument which provides a user-defined function (UDF). This function "knows" what to do with the data. There are two types of functions: (i) functions which are called in an internal loop and take/return data values, (ii) functions which are called only once and take/return collections of values (columns or tables). For each operation it is specified which kind of UDF it uses.

* Data. Here we can specify what data has to be processed by the operation (and the corresponding UDF). For many column operations, it is a list of `columns` of the input table. It is assumed that only these columns have to be processed. For many table operations, it is a list of `tables`.

* Model. This argument of an operation is intended for providing additional parameter for data processing. The model object is passed to UDF which has to know how to use it. It can be as simple as one value and as complex as a trained data mining model. It can be a tuple, dictionary or an arbitrary Python object. A tuple will be unpacked in a list of positional arguments of UDF. A dictionary will be unpacked into a list of keyword arguments. An object will be passed as one positional argument.
