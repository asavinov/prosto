# Column-SQL

## What is Column-SQL

Column-SQL is a query language based on the principles of column-orientation where both columns and tables are main elements of data representation and processing (rather than only tables in the relational and other set-oriented models). In Column-SQL, we process data by mainly deriving new columns from the data stroed in other columns in this or other tables. This approach is simpler and more natural than set-oriented query langauges where we derive tables from other tables. The main problem with traditional set-oriented query languages is that we frequently do not need to derive new tables at all and do it just because there is no choice.

## Column-SQL structure

Column-SQL syntax at high level is intended for describing a flow of data from source columns (or tables) to newly defined target columns (or tables). It is not intended for describing complex workflows but rather short fragments of such workflows. If we define many such short statements then they together are treated as a complex workflow with dependencies. At high level, any Column-SQL statement starts from an operation name followed by a sequence of data elements (table and/or column) separated by the arrow symbol `->`.

    <OPERATION NAME> <TABLE NAME> ( <COLUMN NAME>, ...) -> <TABLE NAME> ( <COLUMN NAME>, ...) ->

Note that between arrows we use a generic syntax for specifying data elements we want to process which is a table name followed by a sequence of its column names in parantheses:

    <TABLE NAME> ( <COLUMN NAME 1>, <COLUMN NAME 2>, ...)

Note that both table name and column list could be empty and there could be single name specified treated either as column or table depending on the operation.

How the data elements bewteen arrows are interpreted depends on the operation and it will be described in the next sections. Here we give only one example of how a new calculated column could be defined which derives its values from two other columns in this same table:

    CALC  My_existing_table(A, B) -> my_new_column

This statement will add a new column to the `My_existing_table` by processing data in columns `A` and `B`. Note that the arrow here means that data from these two source columns flows to the new target column. In this case, this flow is very simply (each output value is computed from two input values) but other operations allows us to link tables and aggregate data from other tables.

According to the concept-oriented model of data, column-orientation means using mathematcial *functions* for representing the data and inferring new data. In particular, this means that how exactly we compute values of new columns is specified by functions. In Prosto, functions needed to compute columns are Python functions which are associated with each Column-SQL statement. Attaching a (Python) function is performed when we add a statement to the Prosto context:

```python
translate_column_sql(ctx, 
    "CALC  My_existing_table(A, B) -> my_new_column", 
    lambda x: x['A'] + x['B']
)
```

The Python function passed to this statement expects a row with two fields in its data argument which are added and the result returned as a new column value.

Alternatively, the function can be passed within Column-SQL statement after the `FUNC` keyword:

```python
translate_column_sql(ctx, 
    "CALC  My_existing_table(A, B) -> my_new_column FUNC lambda x: x['A'] + x['B']" 
)
```

Functions may take an additional (static) argument which can be as simple as one number and as complex as a neural network (trained) model for computing forecasts:

```python
translate_column_sql(ctx, 
    "CALC  My_existing_table(A, B) -> my_new_column", 
    lambda x, **m: x['A'] + x['B'] + param,
    {'param': 1.0}
)
```

It is similar to how `apply` works in `pandas` (and actually it relies on it in its implementation) but it is different from how `map` operation works because a calculated column does not add any new table while `map` computes a new collection (which makes computations less efficient). 

This approach is somewhat similar to spreadsheets with the difference that new columns depend on only one coordinate - other columns - while cells in spreadsheets depend on two coordinates - row and column addresses. The columns defined in some Column-SQL statements can be then used in other statements and the system will evaluate them based on these dependencies.

Column-SQL statements are not executed immediately but rather are simply translated and added to the context. This workflow is evaluated as follows:

```python
ctx.run()
```

In the next sections, we describe operations provided by Column-SQL.

## POPULATE operation for importing data

```python
df = pd.DataFrame({'A': [1, 2, 3]})
table_csql = "TABLE  My_table (A)"
translate_column_sql(ctx, table_csql, lambda **m: df)
```

## CALCULATE operation (instead of map operation)

The purpose of the CALCULATE operation is to create a new column in a table using data in other columns in this same table.

In this example, a `new_column_name` will be created and attached to the existing `My_table`: 

```python
translate_column_sql(ctx, 
    "CALC  My_table (A) -> new_column", 
    lambda x: float(x)
)
```

Note that we specify the input column `A` for our function and its value will be passed to the lambda function. The lambda function will be evaluated for each row of the table and its outputs will be stored as a new column. 

We can specify more input columns for calculating values of the new output column. In addition, it is possible to pass an object with parameters to the function:

```python
translate_column_sql(ctx, 
    "CALC  My_table (A, B) -> new_column", 
    lambda x, **m: x['A']+x['B']+param, model={"param": 5}
)
```

This query will compute a column where each value is the sum of values in columns `A`, `B` plus constant 5. 

## Compute column

The `COMPUTE` operation does the same as the `CALCULATE` except that its function gets whole columns rather than individual rows. The only difference is that the lambda function has to be implemented differently because its arguments are pandas Series.

## LINK operation (instead of join)

The purpose of a link column is to store references to records in another table. Link columns are not valuable by themsevles but they can be then used in other operations to access data from different tables. In this sense, it is a main means of connectivity analogous to joins in the relational model.

Given two existing tables `Facts` and `Groups`, we can define a link from the first one to the second one as follows:

```python
translate_column_sql(ctx, "LINK  Facts (A) -> link_column -> Groups (A)")
```

The `link_column` will be created in the `Facts` table by storing references to the records in the `Groups` table. The criterion of matching records is equality of columns `A` in these two tables.

The main use of link columns is in *column paths* which are sequences of simple column names following links between tables. For example, now we could use the column path `Facts::link_column::target_column` to reference `target_column` from table `Groups` in the context of table `Facts`. Link columns are also used as grouping criteria for aggregation.

## ROLL operation (instead of over-partition)

Like `CALCULATE` operation, `ROLL` operation adds a new column to the same table where input columns are. However, each value of the new column is computed from many rows of this table and not one row. Thus `ROLL` operation aggregates data in many rows in the selected columns.   

```python
translate_column_sql(ctx, 
    "ROLL  My_table (A) -> roll_column WINDOW 2", 
    lambda x: x.sum()
)
```

Each value in the `roll_column` will be computed as the sum of 2 values in the `A` column: one from this record and one from the previous record. The window length is specified in the `WINDOW` parameter. Currently, the logic of grouping logic is equivalent to that of the rolling aggregation in `pandas`. 

## AGGREGATE operation (instead of groupby)

The purpose of the `AGGREGATE` operation is to create a column each value of which aggregates data in several rows of another table. In this sense, it is an analogue of the `groupby` operation in SQL. Its main difference form `groupby` is that a new aggregated column is added directly to the table with groups and no new table is created.

```python
translate_column_sql(ctx,
    "AGGREGATE  Facts (M) -> link_column -> Groups (Aggregate)",
    lambda x, bias, **model: x.sum() + bias,
    {"bias": 0.0}
)
```

This statement adds an `Aggregate` column to the existing table `Groups`. Each value of this aggregate column is the sum of values in the `M` column for several records. All these records belonging to one group reference same record in the `Facts` table using the existing `link_column`.

## FILTER operation (instead of select)

This operation is intended for filtering a table. However, its main difference form the conventional `SELECT` is that a new (filtered) table does not include any columns from the original table. Instead, it it creates a link column and references the selected records from the original (base) table.

In the current implementation, filter conditions are not specified in the operation itself and a boolean column in the base table is needed. The filtered table will include only records for which this column stores true values. In the `FILTER` statement, it is necessary to specify the base table name and the boolean column used for selection:

```python
translate_column_sql(ctx, "FILTER BaseTable (filter_column) -> super -> FilteredTable")
```

This operation creates a new `FilteredTable` and a link column from the `FilteredTable` to the `BaseTable`. This link column in this example is called `super` (because the filtered table is a subset of the base table). 

Note that we can treat the filtered table as a subset of the base table with all the original columns although they are not copied to the new table. We say that the base table columns are inherited by filtered tables.

## PRODUCT operation

The purpose of the `PRODUCT` operation is to create a new table with all combinations of records from the source tables. It also will support filter which is currently not implemented. The new product table will create link columns to every of the source tables and will not contain the source columns.

```python
translate_column_sql(ctx, "PRODUCT  Table_1; Table_2 -> t1; t2 -> Product")
```

The first part of the statement (before first arrow) is a list of source tables (separated by a colon). The second part (between arrows) is a list of the link column names which will be created. The last element `Product` is a name of the product table.

If columns from a source table need to be accessed in some other operation then it is done by means of the link columns as a column path like `Table_1::t1::source_column`.

Although the product operation looks analogous to join, it is has much narrow application scope. It is used mainly for multidimensional analysis (OLAP) and not for connectivity like join. If it is necessary to connect tables, then LINK operation should be used. It is a conceptual difference between the concept-oriented model relying on mathematical functions and the relational model relying on mathematical sets.
