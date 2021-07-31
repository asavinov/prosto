from typing import Union, Any, List, Set, Dict, Tuple, Optional

from prosto.Prosto import *
from prosto.Table import *
from prosto.Column import *
from prosto.TableOperation import *
from prosto.ColumnOperation import *
from prosto.Topology import *
from prosto.column_sql import *

import logging
log = logging.getLogger("prosto")


class Prosto:
    """The class represents a context which stores lists of tables, columns and operations."""

    column_path_separator = "::"

    prosto_no = 0

    def __init__(self, id):

        self.id = id
        if self.id is None:
            self.id = "___prosto___" + str(self.prosto_no)
            self.prosto_no += 1

        self.tables = []
        self.columns = []
        self.operations = []

        self.topology = None
        self.incremental = False

    def __repr__(self):
        return "["+self.id+"]"

    #
    # Table methods
    #

    def create_table(self, table_name, attributes) -> Table:
        """Create a new table with no operation that populates it. The table is supposed to be populated using API."""

        # Create a table definition
        table_def = {
            "id": table_name,
            "attributes": attributes,
        }
        table = Table(self, table_def)
        self.add_table(table)

        return table

    def get_table(self, table_name) -> Table:
        """Find a table with the specified name"""
        if not table_name: return None
        return next((x for x in self.tables if x.id == table_name), None)

    def get_tables(self, table_names) -> List[Table]:
        """Get a list of tables with the specified names"""
        if not table_names: return []
        if isinstance(table_names, str):
            table_names = [table_names]
        tables = filter(lambda x: x.id in table_names, self.tables)
        return list(tables)

    def get_type_table(self, table_name, column_name) -> str:
        """Get type table (name) for its specified column or attribute."""
        if not table_name: return None

        column = self.get_column(table_name, column_name)
        is_attribute = self.has_attribute(table_name, column_name)

        if column:
            # Default
            type_table_name = column.definition.get("type")

            # Determine column operation type
            ops = self.get_column_operations(table_name, column_name)
            op = ops[0] if len(ops) > 0 else None

            if not op:
                pass

            elif op.operation.lower().startswith("merg"):
                # Merge columns do not have type in their definition - they provide only a column path
                # So need to reconstruct the type by following this path
                column_path = op.get_columns()
                type_table_name = table_name
                for i, column_name in enumerate(column_path):
                    # Find type table of one segment which has to be a link except for maybe last segment
                    type_table_name = self.get_type_table(type_table_name, column_name)
                    if not type_table_name:
                        break

            elif op.operation.lower().startswith("link"):
                # Type is part of column definition (not operation) so we simply read it
                pass

            else:
                # It is expected to be None but type field is also expected to be absent
                pass

        elif is_attribute:
            # Default
            type_table_name = None

            # Determine table operation type
            ops = self.get_table_operations(table_name)
            op = ops[0] if len(ops) > 0 else None

            if not op:
                None

            elif op.operation.lower().startswith("filt"):
                # filter table, type is stored in the list of its tables in the definition (one table is possible)
                # It is always the first and the only table
                tables = op.get_tables()
                if not tables:
                    raise ValueError("Table filter operation must specify one base table in the 'tables' field.".format())
                type_table_name = tables[0]

            elif op.operation.lower().startswith("prod"):
                # product table, types are stored in the list of tables in the definition (the order corresponds to the order of attributes)
                # We need to find the table which corresponds to the attribute index in the list
                tables = op.get_tables()
                if not tables:
                    raise ValueError("Table filter operation must specify one base table in the 'tables' field.".format())

                table = self.get_table(table_name)
                attributes = table.definition.get("attributes", [])
                index = attributes.index(column_name)

                type_table_name = tables[index]

            else:
                pass

        else:
            raise ValueError("Name '{}' not found. It is neither column nor attribute".format(column_name))

        return type_table_name

    def get_type_tables(self, table_name, column_names) -> List[str]:
        """
        Get a sequence (list) of table names which are types of the specified column path.
        Each table in this sequence is a type of the corresponding column segment.
        """
        if not table_name: return None

        type_tables = list()
        main_table_name = table_name
        for column_name in column_names:
            type_table_name = self.get_type_table(main_table_name, column_name)
            type_tables.append(type_table_name)
            main_table_name = type_table_name

        return type_tables

    def remove_table(self, table_name) -> Table:
        """
        Remove the specified table if it exists or return None otherwise.
        Note that the removed table might be produced or consumed by some operations.
        The operation(s) that produce this table is also removed.
        """
        table = self.get_table(table_name)
        if table is None:
            return None
        self.tables.remove(table)

        # Remove operation(s) which generate this table
        ops = self.get_table_operations(table_name)
        for op in ops:
            self.operations.remove(op)

        return table

    def add_table(self, table: Table) -> Table:
        """
        Add table. If a table with this name table then it is removed before adding.
        """
        table_name = table.id
        self.remove_table(table_name)
        self.tables.append(table)
        return table

    #
    # Column methods
    #

    def has_attribute(self, table_name, attribute_name) -> bool:
        """Check if the attribute exists in the table"""
        table = self.get_table(table_name)
        if not table:
            return False
        # In case of no attributes, we assume that attributes will be populated during execution and the specified attribute will be there
        if not table.definition.get("attributes"):
            return True
        attributes = table.definition.get("attributes", [])
        return attribute_name in attributes

    def get_column(self, table_name, column_name) -> Column:
        """Find a column the specified name"""
        if not table_name: return None
        if not column_name: return None
        return next((x for x in self.columns if x.id == column_name and x.table.id == table_name), None)

    def get_columns(self, table_name, column_names=None) -> List[Column]:
        """Get a list of columns with the specified names. All columns belong to one table."""
        if not table_name: return None
        if not column_names:
            all_columns = filter(lambda x: x.table.id == table_name, self.columns)
            return all_columns
        if isinstance(column_names, str):
            column_names = [column_names]
        columns = filter(lambda x: x.id in column_names and x.table.id == table_name, self.columns)
        return list(columns)

    def remove_column(self, table_name, column_name) -> Column:
        """
        Remove the specified column if it exists or return None otherwise.
        Note that the removed column might be produced or consumed by some operations.
        The operation(s) that produce this column is also removed.
        """
        column = self.get_column(table_name, column_name)
        if column is None:
            return None
        self.columns.remove(column)

        # Remove operation(s) which generate this column
        ops = self.get_column_operations(table_name, column_name)
        for op in ops:
            self.operations.remove(op)

        return column

    def add_column(self, column: Column) -> Column:
        """
        Add column. If a column with this name exists then it is removed before adding.
        """
        table_name = column.table.id
        column_name = column.id
        self.remove_column(table_name, column_name)
        self.columns.append(column)
        return column

    #
    # Operations
    #

    def get_table_operations(self, table_name) -> List[TableOperation]:
        """Find operations which generate the specified table. Such operations have this table name in its outputs."""
        return [x for x in self.operations if isinstance(x, TableOperation) and table_name in x.get_outputs()]

    def get_column_operations(self, table_name, column_name) -> List[ColumnOperation]:
        """Find operations which generate the specified column. Such operations have this column name in its outputs as well as the specified table name (each column operation has a table field)."""
        return [x for x in self.operations if isinstance(x, ColumnOperation) and column_name in x.get_outputs() and table_name == x.definition.get("table")]

    #
    # Table operations
    #

    def populate(
            self,
            table_name, attributes,
            func, tables=None, model=None
    ) -> Table:
        """
        Create a new populate table.

        The table will be populated with the data returned by the UDF specified as a parameter.
        The method can be used to populate source tables with the data from external data sources.
        The method can be used to process data in input tables and then these input tables have to be specified in the paraneters and their data will be passed to UDF.
        """

        # Create a table definition
        table_def = {
            "id": table_name,
            "attributes": attributes,
        }
        table = Table(self, table_def)
        self.add_table(table)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "populate",

            "outputs": [table_name],

            "function": func,
            "tables": tables,
            "model": model,
            "input_length": "table",
        }
        operation = TableOperation(self, operation_def)
        self.operations.append(operation)

        return table

    def product(
            self,
            table_name, attributes,
            tables
    ) -> Table:
        """
        Create a new product table.

        The output table is the Cartesian product of the input tables and consists of all combinations of their records.
        Its attributes are links to the input tables.
        """

        # Create a table definition
        table_def = {
            "id": table_name,
            "attributes": attributes,
        }
        table = Table(self, table_def)
        self.add_table(table)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "product",

            "outputs": [table_name],

            "tables": tables,
        }
        operation = TableOperation(self, operation_def)
        self.operations.append(operation)

        return table

    def filter(
            self,
            table_name, attributes,
            func, tables, columns=None
    ) -> Table:
        """
        Create a new filter table.

        The output table consists of a subset of records from the input table.
        It has one attribute which is a link to the input table.
        """

        # Create a table definition
        table_def = {
            "id": table_name,
            "attributes": attributes,
        }
        table = Table(self, table_def)
        self.add_table(table)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "filter",

            "outputs": [table_name],

            "function": func,
            "tables": tables,
            "columns": columns,
        }
        operation = TableOperation(self, operation_def)
        self.operations.append(operation)

        return table


    def project(
            self,
            table_name, attributes,
            tables, columns=None
    ) -> Table:
        """
        Create a new project table.

        The output table consists of all unique combinations of the specified columns in the input table.
        The definition is very similar to a link column definition (but without link column name
        because it will not be created). It is an auxiliary operation not supposed to be used independently
        (like merge).
        """

        # TODO: Attributes are optional. If absent, then use source columns.

        # Create a table definition
        table_def = {
            "id": table_name,
            "attributes": attributes,
        }
        table = Table(self, table_def)
        self.add_table(table)

        # Create operation definition
        operation_def = {  # Project table
            "id": None,
            "operation": "project_table",

            "outputs": [table_name],  # Target table
            "linked_columns": attributes,

            "tables": tables,  # Source table
            "columns": columns,
        }
        operation = TableOperation(self, operation_def)
        self.operations.append(operation)

        return table

    #
    # Column operations
    #

    def compute(
            self,
            name, table,
            func, columns=None, model=None
    ) -> Column:
        """
        Create a new calculate column.

        The output values are computed from the input values of the same row using the specified UDF.
        UDF is called one time and returns a new column with all the value computed from the input columns passed in the parameters.
        """

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.add_column(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "compute",

            "table": table,
            "outputs": [name],

            "function": func,
            "columns": columns,
            "model": model,
            "input_length": "column",
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

    def calculate(
            self,
            name, table,
            func, columns=None, model=None
    ) -> Column:
        """
        Create a new calculate column.

        The output values are computed from the input values of the same row using the specified UDF.
        UDF is called as many times as there are input rows in the table and each time returns one value calculated from the input values passed in the parameters.
        """

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.add_column(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "calculate",

            "table": table,
            "outputs": [name],

            "function": func,
            "columns": columns,
            "model": model,
            "input_length": "value",
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

    def link(
            self,
            name, table, type,
            columns, linked_columns=None
    ) -> Column:
        """
        Create a new link column.

        The output values reference matching rows in another (linked) table.
        Two rows match if their specified columns are equal.
        """

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
            "type": type,
        }
        column = Column(self, definition)
        self.add_column(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "link",

            "table": table,
            "outputs": [name],

            "columns": columns,
            "linked_columns": linked_columns,
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

    def merge(
            self,
            name, table,
            columns
    ) -> Column:
        """
        Create a new merge column.

        A merge column materializes a column in some other table which is accessed via a link path.
        The output values are equal to the linked values stored in another table.
        The first segment of the link path starts from this table and the last segment represents a column to be merged (copied to the output column).
        """

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.add_column(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "merge",

            "table": table,
            "outputs": [name],

            "columns": columns,
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

    def roll(
            self,
            name, table,
            window, link,
            func, columns=None, model=None
    ) -> Column:
        """
        Create a new rolling aggregation column.

        Each output value is equal to one (aggregated) value computed from several rows (window) of this table.
        """

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.add_column(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "rolling",

            "table": table,
            "outputs": [name],

            # How to group
            "window": window,
            "link": link,

            # How to aggregate
            "function": func,
            "columns": columns,
            "model": model,
            "input_length": "column",
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

    def aggregate(
            self,
            name, table,
            tables, link,
            func, columns=None, model=None
    ) -> Column:
        """
        Create a new aggregate column.

        Each output value is equal to one (aggregated) value computed from several rows (group) of another (fact) table.
        """

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.add_column(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "aggregate",

            "table": table,
            "outputs": [name],

            # How to group
            "tables": tables,
            "link": link,

            # How to aggregate
            "function": func,
            "columns": columns,
            "model": model,
            "input_length": "column",

            "initial_value": 0.0, # Pre-process like initial value
            "fillna_value": 0.0,  # Postprocess
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

    def discretize(
            self,
            name, table,
            columns=None, model=None
    ) -> Column:
        """
        Create a new discretize column.

        Each output value is produced by the specified discretization function.
        """

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.add_column(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": "discretize",

            "table": table,
            "outputs": [name],

            "columns": columns,
            "model": model,
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

    #
    # Column-SQL
    #

    def column_sql(self, query: str, func=None, args=None):
        # Parse query and extract parameters
        op, entries, func_str, args_str, win_str = parse_column_sql(query)

        if func is None:
            func = func_str
        if args is None:
            args = args_str

        if op.lower().startswith("tabl"):
            table = entries[0][0]
            attributes = entries[0][1:]

            definition = self.populate(
                table, attributes=attributes,
                func=func, tables=None, model=args
            )
        elif op.lower().startswith("calc"):
            table = entries[0][0]
            columns = entries[0][1:]

            name = entries[1][0]

            definition = self.calculate(
                name, table,
                func=func, columns=columns, model=None if not args else args
            )
        elif op.lower().startswith("roll"):
            table = entries[0][0]
            columns = entries[0][1:]

            name = entries[1][0]

            definition = self.roll(
                name=name, table=table,
                window=win_str, link=None,
                func=func, columns=columns, model=None if not args else args
            )
        elif op.lower().startswith("link"):
            table = entries[0][0]
            columns = entries[0][1:]

            name = entries[1][0]

            type_table = entries[-1][0]
            linked_columns = entries[-1][1:]

            definition = self.link(
                name=name, table=table, type=type_table,
                columns=columns, linked_columns=linked_columns
            )
        elif op.lower().startswith("proj"):
            table = entries[0][0]
            columns = entries[0][1:]

            name = entries[1][0]

            type_table = entries[-1][0]
            linked_columns = entries[-1][1:]

            # The operation is treated as creation of a target (project) tables with data
            definition = self.project(
                table_name=type_table, attributes=linked_columns,
                tables=[table], columns=columns
            )
            # This definition will be used by the project operation to get additional parameters
            definition2 = self.link(
                name=name, table=table, type=type_table,
                columns=columns, linked_columns=linked_columns
            )
        elif op.lower().startswith("aggr"):
            fact_table = entries[0][0]
            fact_columns = entries[0][1:]

            link_path = entries[1][0]

            group_table = entries[-1][0]
            agg_column = entries[-1][1]

            definition = self.aggregate(
                name=agg_column, table=group_table,
                tables=fact_table, link=link_path,
                func=func, columns=fact_columns, model=None if not args else args
            )
        elif op.lower().startswith("filt"):
            table = entries[0][0]
            columns = entries[0][1:]

            name = entries[1][0]

            filtered_table = entries[-1][0]

            if not func:
                # Assume that the (only) base column is a boolean column for the filter (e.g., resulted from a predicate function)
                definition = self.filter(
                    table_name=filtered_table, attributes=[name],
                    func=None, tables=table, columns=columns
                )
            else:
                # The predicate function is provided and hence we add a calculate operation which materializes it as a boolean column
                filter_column_name = "__" + filtered_table + "__"
                definition = self.calculate(
                    name=filter_column_name, table=table,
                    func=func, columns=columns, model=None if not args else args
                )
                definition = self.filter(
                    table_name=filtered_table, attributes=[name],
                    func=None, tables=table, columns=filter_column_name
                )
        elif op.lower().startswith("prod"):
            sep = ";"

            tables_str = entries[0][0]
            tables = [x.strip() for x in tables_str.split(sep)]

            columns_str = entries[1][0]
            columns = [x.strip() for x in columns_str.split(sep)]

            name = entries[-1][0]

            definition = self.product(
                table_name=name, attributes=columns,
                tables=tables
            )
        else:
            raise NotImplementedError(f"Column-SQL operation {op} not implemented or not known.")

        return definition

    #
    # Execution
    #

    def translate(self) -> Topology:
        self.topology = Topology(self)
        self.topology.translate()
        return self.topology

    def run(self) -> None:
        """Execute the whole workflow."""
        log.info("Start executing workflow '{}'.".format(self.id))

        # Translate
        self.translate()

        # Execute operations in the graph
        for layer in self.topology.layers:
            # Execute operations in one layer
            for op in layer:
                operation = op.definition.get("operation")

                if isinstance(op, TableOperation):
                    outputs = op.get_outputs()
                    log.info("===> Start table population: id '{}', type = '{}', tables {}".format(op.id, operation, outputs))
                    op.evaluate()
                    log.info("<=== Finish table population".format())

                elif isinstance(op, ColumnOperation):
                    columns = op.get_columns()
                    log.info("---> Start column evaluation: id = '{}', type = '{}', columns {}".format(op.id, operation, columns))
                    op.evaluate()
                    log.info("<--- Finish column evaluation".format())

                else:
                    log.warning("Unknown element '{}' in the topology '{}'.".format(op.id, self.id))

        # Clear change status of all elements
        for tbl in self.tables:
            tbl.data.clear_change_status()
            tbl.data.gc()

        log.info("Finished executing workflow '{}'.".format(self.id))


if __name__ == "__main__":
    pass
