# Column paths

## Link columns

A link column stores row identifiers of another table. Its purpose is to provide access to the data stored in another table. For example, a `Persons` table might store such attributes like name directly in this table while address information could be stored in another table called `Addresses`. In this case, the `Persons` table should have a column which references records of the `Addresses` table. For example, this link column could be called `address`. In contrast to other columns, its values are not used directly but rather are used to access values in the referenced table.

## Column paths

If a table has a link column then it can be used to access columns in the referenced table. This is done by specifying a column path which is a sequence of link columns where each next column starts from the table where previous column ends. The last column in a column path is a normal table column or attribute the data of which has to be processed. Syntactically, `Prosto` uses double colon to separate segments in a column path. For example, in order to access a street column from the `Persons` table we write the following column path: `address::street`. This column path starts from the `Persons` table and ends in the `Addresses` table.

## Defining link columns

Link columns are defined by means of the `link` operation. Given two tables, this operation will produce a new link column the values of which will reference the second (target) table from the first table. The criterion of matching is equality of the specified columns. For example, in Column-SQL a link column could be defined as follows:

```python
ctx.column_sql("LINK  SourceTable(A, B) -> link_column -> TargetTable (A, B)")
```

Once this link column has been defined, it can be used in all operations where the `SourceTable` columns are used in the same way as simple columns, for example: `link_column::C` for accessing the `C` columns of the `TargetTable`.

Link columns are also automatically created by the `project` operation. This operation not only projects a source table by producing a new target table, but also creates a link between these two tables. For example, if the `TargetTable` was not available, then it could be created by projecting the `SourceTable` by finding all unique combinations of the `A` and `B` attributes:

```python
ctx.column_sql("PROJECT  SourceTable(A, B) -> link_column -> TargetTable (A, B)")
```

## Use of column paths

Column paths can be used where a computation needs to be done with some data in another table. For example, we might want to define a calculate column using data in a referenced table:

```python
ctx.column_sql(
    "CALCULATE  Persons(position::salary) -> adjusted_salary",
    lambda x: x['position::salary'] * 1.1
)
```

Here the `salary` is stored in a table with all existing positions while `Persons` have a link to the position.

Link columns are used also in `aggregate` columns for grouping by assuming that all referencing one record means belonging to one group. For example, we could find mean age for each position as follows:

```python
ctx.column_sql(
    "AGGREGATE  Persons (age) -> position -> Positions (mean_age)",
    lambda x: x.mean()
)
```
