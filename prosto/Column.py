import json

from prosto.utils import *

from prosto.Prosto import *


class Column:
    """The class represents one column definition."""

    column_no = 0

    def __init__(self, prosto, definition):
        """
        Create a new column object using its definition.

        :param prosto: Prosto context object this table belongs to
        :param definition: Column definition as a dict
        """

        # Assign id
        self.id = definition.get("id", None)
        if self.id is None:
            self.id = "___column___" + str(Column.column_no)
            definition["id"] = self.id
            Column.column_no += 1

        self.prosto = prosto
        self.definition = definition

        # Resolve from-table name
        table_name = self.definition.get("table", "")
        self.table = self.prosto.get_table(table_name)

        # Resolve to-table name
        table_name = self.definition.get("type", "")
        if table_name:  # It is link column (the type-table must already exist)
            self.type = self.prosto.get_table(table_name)
        else:  # Primitive type
            self.type = None


    def __repr__(self):
        return "[" + self.table.id + "::" + self.id+"]"

    def evaluate(self) -> None:
        """Find a column operation which generates this column and execute it."""
        col_ops = self.prosto.get_column_operations(self.table.id, self.id)
        col_ops[0].evaluate()


if __name__ == "__main__":
    pass
