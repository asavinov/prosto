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

    def resolve_column_to_path(self, column_name) -> List[str]:
        """
        Find the specified (simple) column name in this table as well as all its base (parent) tables
        and return a column path to it. A base table is used in filter and product tables.
        The last segment in the returned path is equal to the column argument.
        """
        table_name = self.id

        # 1. Check the existence in the current node, if found, then return it, if not then
        is_attribute = self.prosto.has_attribute(table_name, column_name)
        if is_attribute:  # Found
            return [column_name]

        column = self.prosto.get_column(table_name, column_name)
        column_ops = self.prosto.get_column_operations(table_name, column_name)
        if column and column_ops:  # Found a column with definition
            return [column_name]

        # No such column/attribute in this table

        # 2. Find a list of all direct links and their parent (type tables)
        table_ops = self.prosto.get_table_operations(table_name)
        if len(table_ops) > 1:
            raise ValueError("Several operations generate one table '{}'".format(table_name))
        if len(table_ops) == 0:
            raise ValueError("No operations found that generates this table '{}'".format(table_name))
        table_op = table_ops[0] if len(table_ops) > 0 else None

        if table_op.operation.lower().startswith("filt") or table_op.operation.lower().startswith("prod"):
            base_attribute_names = self.definition.get("attributes", [])

            # Find its parent tables (where we will search for our missing column)
            base_table_names = table_op.get_tables()
            if not base_table_names:
                raise ValueError("Table filter operation must specify one base table in the 'tables' field.".format())
            #base_tables = self.prosto.get_tables(base_table_names)

            if len(base_attribute_names) != len(base_table_names):
                raise ValueError("Table '{}' has different number of base attributes and base tables in its definition.".format(table_name))

        else:
            return []

        # 3. Call this same function by passing one of these parent tables in a loop
        for i, base_table_name in enumerate(base_table_names):
            base_table = self.prosto.get_table(base_table_name)
            rest_path = base_table.resolve_column_to_path(column_name)
            if rest_path:  # Found
                # TODO: Here we return the first path found, but maybe we should detect conflicts if more than one path found
                return [base_attribute_names[i]] + rest_path

        return []

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
