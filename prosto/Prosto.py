from typing import Union, Any, List, Set, Dict, Tuple, Optional

from prosto.Prosto import *
from prosto.Table import *
from prosto.Column import *
from prosto.TableOperation import *
from prosto.ColumnOperation import *
from prosto.Topology import *

import logging
log = logging.getLogger('prosto')


class Prosto:
    """The class represents a context which stores lists of tables, columns and operations."""

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
        return '['+self.id+']'

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
        self.tables.append(table)

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

    #
    # Column getters
    #

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

    #
    # Operations
    #

    def get_table_operations(self, table_name) -> List[TableOperation]:
        """Find operations which generate the specified table. Such operations have this table name in its outputs."""
        return [x for x in self.operations if isinstance(x, TableOperation) and table_name in x.definition.get("outputs", [])]

    def get_column_operations(self, table_name, column_name) -> List[ColumnOperation]:
        """Find operations which generate the specified column. Such operations have this column name in its outputs as well as the specified table name (each column operation has a table field)."""
        return [x for x in self.operations if isinstance(x, ColumnOperation) and column_name in x.definition.get("outputs", []) and table_name == x.definition.get("table")]

    #
    # Table operations
    #

    def populate(
            self,
            table_name, attributes,
            func, tables=None, model=None, input_length='table'
    ) -> Table:
        """Create a new populate table."""

        # Create a table definition
        table_def = {
            "id": table_name,
            "attributes": attributes,
        }
        table = Table(self, table_def)
        self.tables.append(table)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'populate',

            "outputs": [table_name],

            "function": func,
            "tables": tables,
            "model": model,
            "input_length": input_length,
        }
        operation = TableOperation(self, operation_def)
        self.operations.append(operation)

        return table

    def product(
            self,
            table_name, attributes,
            tables
    ) -> Table:
        """Create a new product table."""

        # Create a table definition
        table_def = {
            "id": table_name,
            "attributes": attributes,
        }
        table = Table(self, table_def)
        self.tables.append(table)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'product',

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
        """Create a new filter table."""

        # Create a table definition
        table_def = {
            "id": table_name,
            "attributes": attributes,
        }
        table = Table(self, table_def)
        self.tables.append(table)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'filter',

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
            link, tables
    ) -> Table:
        """Create a new project table."""

        # Create a table definition
        table_def = {
            "id": table_name,
            "attributes": attributes,
        }
        table = Table(self, table_def)
        self.tables.append(table)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'project',

            "outputs": [table_name],

            "tables": tables,
            "link": link,
        }
        operation = TableOperation(self, operation_def)
        self.operations.append(operation)

        return table

    #
    # Column operations
    #

    def calculate(
            self,
            name, table,
            func, columns=None, model=None, input_length='column'
    ) -> Column:
        """Create a new calculate column."""

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.columns.append(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'calculate',

            "table": table,
            "outputs": [name],

            "function": func,
            "columns": columns,
            "model": model,
            "input_length": input_length,
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

    def link(
            self,
            name, table, type,
            columns, linked_columns=None
    ) -> Column:
        """Create a new link column."""

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
            "type": type,
        }
        column = Column(self, definition)
        self.columns.append(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'link',

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

        A merge column materializes a column in some other table which is accessed via a column path.
        The column path is a sequence of link columns (except for maybe the last one).
        The first segments starts from this table and the last segment represents a column to be merged.
        """

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.columns.append(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'merge',

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
            window,
            func, columns=None, model=None, input_length='column'
    ) -> Column:
        """Create a new rolling aggregation column."""

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.columns.append(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'rolling',

            "table": table,
            "outputs": [name],

            # How to group
            "window": window,

            # How to aggregate
            "function": func,
            "columns": columns,
            "model": model,
            "input_length": input_length,
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

    def aggregate(
            self,
            name, table,
            tables, link,
            func, columns=None, model=None, input_length='column'
    ) -> Column:
        """Create a new aggregate column."""

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.columns.append(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'aggregate',

            "table": table,
            "outputs": [name],

            # How to group
            "tables": tables,
            "link": link,

            # How to aggregate
            "function": func,
            "columns": columns,
            "model": model,
            "input_length": input_length,

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
        """Create a new discretize column."""

        # Create a column definition
        definition = {
            "id": name,
            "table": table,
        }
        column = Column(self, definition)
        self.columns.append(column)

        # Create operation definition
        operation_def = {
            "id": None,
            "operation": 'discretize',

            "table": table,
            "outputs": [name],

            "columns": columns,
            "model": model,
        }
        operation = ColumnOperation(self, operation_def)
        self.operations.append(operation)

        return column

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

        # Translate if necessary
        if self.topology is None:
            self.translate()

        # Execute operations in the graph
        for layer in self.topology.layers:
            # Execute operations in one layer
            for op in layer:
                if isinstance(op, TableOperation):
                   op.evaluate()
                elif isinstance(op, ColumnOperation):
                    op.evaluate()
                else:
                    log.warning("Unknown element '{}' in the topology '{}'.".format(op.id, self.id))

        # Clear change status of all elements
        for tbl in self.tables:
            tbl.data.clear_change_status()
            tbl.data.gc()

        log.info("Finished executing workflow '{}'.".format(self.id))


if __name__ == "__main__":
    pass
