from typing import Union, Any, List, Set, Dict, Tuple, Optional
import json

from prosto.utils import *

from prosto.Prosto import *
from prosto.Column import *
from prosto.Data import *


class Table:
    """The class represents one table. Table data is stored in columns."""

    table_no = 0

    def __init__(self, prosto, definition):
        """
        Create a new table object using its definition.

        :param prosto: Prosto context object this table belongs to
        :param definition: Table definition as a dict
        """

        # Assign id
        self.id = definition.get('id', None)
        if self.id is None:
            self.id = "___table___" + str(Table.table_no)
            definition['id'] = self.id
            Table.table_no += 1

        self.prosto = prosto
        self.definition = definition

        # Here we store the real (physical) data for this table (all its attributes and columns)
        self.data = Data(self)

    def __repr__(self):
        return '['+self.id+']'

    def get_data(self) -> pd.DataFrame:
        return self.data.get_df()

    def get_column_data(self, column_name) -> pd.Series:
        return self.get_data()[column_name]

    #
    # Column getters
    #

    def get_column(self, column_name) -> Column:
        """Find a column definition object with the specified name"""
        return self.prosto.get_column(self.id, column_name)

    def get_columns(self) -> List[Column]:
        """Get a list of all columns of this table."""
        return self.prosto.get_columns(self.id)

    def evaluate(self) -> None:
        """Find a table operation which generates this table and execute it."""
        tab_ops = self.prosto.get_table_operations(self.id)
        tab_ops[0].evaluate()

if __name__ == "__main__":
    pass
