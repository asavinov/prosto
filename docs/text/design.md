# Design

## Data schema

Prosto stores two lists as members of the `Prosto` class`:
* List of table definitions in the `tables` field. A table object is represented by an instance of the `Table` class. 
* List of column definitions in the `columns` field. A column object is represented by an instance of the `Column` class.

A table definition involves such fields as table name and a list of its attributes:

```python
definition = {
    "id": "My table",
    "attributes": ["A", "B"],
}
```

A column definition involves such fields as column name and table name:

```python
definition = {
    "id": "My column",
    "table": "My table",
}
```

## Data

Real data being processed is stored in the `data` field of the `Table` class. The data is represented by an instance of the `Data` class. It relies on `pandas` `DataFrame` object for data representation by storing data for all columns of this table (so `Column` objects do not store any data). 

The `Table` class stores also `pandas` groupby object for each link column which is then used in operations with the link columns.

## Operations

Prosto stores all operation definitions in the `operations` field of the `Prosto` class`. Each operation is a dictionary object with certain structure which is interpreted depending on the operation.  

A operation definition includes the following fields:

```python
definition = {
    "id": "My operaiton",
    "operation": "operation_name",  # Operation name (supported by Prosto)

    "outputs": ["My table"],  # What this operation produces (table or column)

    "tables": ["Table"],  # Source table
    "columns": ["A"],  # Source columns
    
    "function": func,  # UDF
}
```

There can be other fields depending on the operation.

A base class for all operations is `Operation`. It has two subclasses: `TableOperation` and `ColumnOperation`. These classes implement the logic of execution of each operation.

## Dependencies

Each operation has dependencies as tables and columns which must be available before this operation can be executed. The dependencies are computed and returned depending on the operation type and its definition. These methods are implemented in the `Operation` class and its child classes. 

## Topology and translation

Topology represents a graph of operations which are ready to be executed and this object can execute them as one workflow. Translating a topology means generating such a list of operations from their definitions stored in the Prosto context. The translation procedure analyzes the list of operations with their dependencies and produces a graph. This procedure may also add new operations.
