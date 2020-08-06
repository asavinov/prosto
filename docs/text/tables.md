# Table operations

## Populate table

A new table will be populated with data returned by the specified user-defined function. This operation is analogous to `compute` column operation with the difference that a complete table is returned rather than a complete column.

## Product of tables (instead of join)

This table is intended to produce all combinations of rows in other tables. Its main difference from the relational model is that the result table stores links to the rows of the source tables rather than copies of its rows. The result table has as many attributes as it has source tables in its definition. (In contrast, the number of attributes in a relational product is equal to the sum of attributes in all source tables.)

Uses:
* Creating a cube table from dimension tables for multi-dimensional analysis. It is typically followed by aggregating data in the fact table. 

## Filter table (instead of select-where)

It is one of the most frequently used operations. The main difference form conventional implementations is that the result never includes the source table columns. Instead, the result (filtered) table references the selected source rows using an automatically created link column. If it is necessary to use the source table data (and it is almost always the case) then they are accessible via the created link column. 

## Project table (instead of select-distinct)

This operation has these important uses:
* Creating a table with group elements for aggregation because (in contrast to other approaches) it must exist
* Creating a dimension table for multi-dimensional analysis in the case it does not exist

Check out the `project.ipynb` notebook for a working example of the `project` operaiton.

## Range table

Not implemented yet.

This operation populates a table with one attribute which contains values from a range described in the model. A range specification typically has such parameters as `start`, `end`, `step` size (or frequency), `origin` and others depending on the range type.

Links:
* https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.date_range.html
* https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#generating-ranges-of-timestamps
