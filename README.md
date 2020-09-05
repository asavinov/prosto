```
 ____                _        
|  _ \ _ __ ___  ___| |_ ___   _________________________________________________
| |_) | '__/ _ \/ __| __/ _ \ 
|  __/| | | (_) \__ \ || (_) | Functions matter! No map-reduce. No join-groupby.
|_|   |_|  \___/|___/\__\___/  _________________________________________________
```
[![Documentation Status](https://readthedocs.org/projects/prosto/badge/?version=latest)](https://prosto.readthedocs.io/en/latest/?badge=latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://github.com/prostodata/prosto/blob/master/LICENSE)
[![Python 3.6](https://img.shields.io/badge/python-3.6-brightgreen.svg)](https://www.python.org/downloads/release/python-370/)
[![PyPI version](https://badge.fury.io/py/prosto.svg)](https://badge.fury.io/py/prosto)
[![Paper PDF](https://img.shields.io/badge/Paper-PDF-brightgreen.svg)](https://arxiv.org/ftp/arxiv/papers/1911/1911.07225.pdf)

• [**Why Prosto?**](#why-prosto) • [**Quick start**](#quick-start) • [**How to use**](#how-to-use) • [**References**](#references) • [**Documentation**](http://prosto.readthedocs.io/) •

`Prosto` is a Python data processing toolkit to programmatically author and execute complex data processing workflows. Conceptually, it is an alternative to purely *set-oriented* approaches to data processing like map-reduce, relational algebra, SQL or data-frame-based tools like `pandas`.

`Prosto` radically changes the way data is processed by relying on a novel data processing paradigm: concept-oriented model of data [[1]](#1). It treats columns (modelled via mathematical functions) as first-class elements of the data processing pipeline having the same rights as tables. If a traditional data processing graph consists of only set operations than the `Prosto` workflow consists of two types of operations:

* *Table operations* produce (populate) new tables from existing tables. A table is an implementation of a mathematical *set* which is a collection of tuples.

* *Column operations* produce (evaluate) new columns from existing columns. A column is an implementation of a mathematical *function* which maps tuples from one set to another set.

An example of such a `Prosto` workflow consisting of 3 column operations is shown below. The main difference from traditional approaches is that this `Prosto` workflow will not modify any table - it changes only columns. Formally, if traditional approaches apply set operations by generating new sets from already inferred sets, then `Prosto` derives new *functions* from existing functions. In many cases, using functions (column operations) is much simpler and more natural.

![Data processing workflow](docs/images/fig_1.png)

`Prosto` operations are demonstrated in notebooks which can be found in the "notebooks" folder in the main repo. Do your own experiments by tweaking them and playing around with the code: https://github.com/prostodata/prosto/tree/master/notebooks

More detailed information can be found in the documentation: http://prosto.readthedocs.io 

# Why Prosto?

Main motivation:

Processing data using only set operations is counter-intuitive in many quite important cases. In particular, this is why SQL, map-reduce and similar approaches require high expertise. Prosto adds mathemtical *functions* (columns) to its model by significantly simplifying data processing and analysis.

Prosto provides the following unique features and benefits:

* Processing data in multiple tables
* Getting rid of joins
* Getting rid of group-by
* Flexibility via user-defined functions

More info: [Why Prosto?](https://prosto.readthedocs.io/en/latest/text/why.html)

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

More info: [Workflow and operations](https://prosto.readthedocs.io/en/latest/text/workflow.html)

## Defining a table

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

More info: [Table operations](https://prosto.readthedocs.io/en/latest/text/tables.html)

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

More info: [Column operations](https://prosto.readthedocs.io/en/latest/text/columns.html)

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

Although it looks like a normal table, the last column was derived from the data in other columns. If we change input data, then we can again run this workflow and the derived column will contain updated results.

The full power of `Prosto` comes from the ability to process data in multiple tables by definining derived links (instead of joins) and then aggregating data based on these links (without groupby). Note that both linking and aggregation do not require and will not produce new tables: only columns are defined and evaluated. For example, we might use column paths like `my_derived_link::my_column` in operations in order to access data in other tables.

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

# References

<a id="1"></a>[1]: A.Savinov. Concept-oriented model: Modeling and processing data using functions, Eprint: [arXiv:1606.02237](https://arxiv.org/ftp/arxiv/papers/1911/1911.07225.pdf) [cs.DB], 2019. https://www.researchgate.net/publication/337336089_Concept-oriented_model_Modeling_and_processing_data_using_functions

