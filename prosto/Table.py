import json

from prosto.utils import *

from prosto.Schema import *
from prosto.Table import *
from prosto.Column import *

import logging
log = logging.getLogger('prosto.table')


class Table:
    """The class represents one table. Table data is stored in columns."""

    table_no = 0

    def __init__(self, schema, definition):
        """
        Create a new table object using its definition.

        :param schema: Schema object this table belongs to
        :param definition: Table definition as a dict
        """

        # Assign id
        self.id = definition.get('id', None)
        if self.id is None:
            self.id = "___table___" + str(Table.table_no)
            definition['id'] = self.id
            Table.table_no += 1

        self.schema = schema
        self.definition = definition

        self.data = None

    def __repr__(self):
        return '['+self.id+']'

    def get_data(self):
        return self.data

    def get_column_data(self, column_name):
        return self.data[column_name]

    #
    # Column getters
    #

    def get_column(self, column_name):
        """Find a column definition object with the specified name"""
        return self.schema.get_column(self.id, column_name)

    def get_columns(self):
        """Get a list of all columns of this table."""
        return self.schema.get_columns(self.id)

    def evaluate(self):
        """Find a table operation which generates this table and execute it."""
        tab_ops = self.schema.get_table_operations(self.id)
        tab_ops[0].evaluate()

if __name__ == "__main__":
    pass
