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
        self.id = definition.get("id", None)
        if self.id is None:
            self.id = "___table___" + str(Table.table_no)
            definition["id"] = self.id
            Table.table_no += 1

        self.prosto = prosto
        self.definition = definition

        # Here we store the real (physical) data for this table (all its attributes and columns)
        self.data = Data(self)

        # A mapping from (link) column/attribute names to the corresponding groupby objects
        self.groupby = {}


    def __repr__(self):
        return "["+self.id+"]"

    def get_df(self) -> pd.DataFrame:
        return self.data.get_df()

    def get_series(self, column_name) -> pd.Series:
        return self.data.get_series(column_name)

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

    def _get_or_create_groupby(self, link_column_name):
        """
        For each link column or attribute, this table object stores a pandas groupby object which is built when this link column is first time used.
        Return or build such a groupby object for the (already evaluated) link column/attribute.
        Currently, link columns/attributes are used in such operations as aggregation and grouped rolling aggregation.
        """
        gb = self.groupby.get(link_column_name)
        if gb is not None:
            return gb

        # Use link column (with target row ids) to build a groupby object (it will build a group for each target row id)
        try:
            # Option 1:
            gb = self.get_df().groupby(link_column_name, sort=False, as_index=True)
            # Option 2:
            #gb = self.get_data().groupby([link_column_name], sort=False, as_index=False)
            # Option 3: group by index - grouping column will be retained via index
            #gb = self.get_data().set_index(link_column_name, append=True).groupby(level=1)

            # Alternatively, we could use target keys or main keys
        except Exception as e:
            raise ValueError("Error grouping input table using the specified column(s). Exception: {}".format(e))

        # TODO: We might want to remove a group for null value (if it is created by the groupby constructor)

        self.groupby[link_column_name] = gb

        return gb


if __name__ == "__main__":
    pass
