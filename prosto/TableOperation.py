from typing import Union, Any, List, Set, Dict, Tuple, Optional
import json

from prosto.utils import *
from prosto.resolve import *

from prosto.Prosto import *
from prosto.Table import *
from prosto.Column import *
from prosto.Operation import *


class TableOperation(Operation):
    """The class represents one table operation."""

    def __init__(self, prosto, definition):
        super(TableOperation, self).__init__(prosto, definition)

    def get_dependencies(self) -> List[Union[Table, Column]]:
        """Get tables and columns this table depends upon."""
        definition = self.definition
        operation = definition.get("operation", "UNKNOWN")

        outputs = self.get_outputs()
        output_table_name = outputs[0]
        output_table = self.prosto.get_table(output_table_name)

        tables = self.get_tables()
        input_tables = self.prosto.get_tables(tables)

        dependencies = []

        dependencies.extend(input_tables)

        if operation.lower().startswith("popu"):
            # We want to evaluate *all* columns of the base table except for those which depend on this table
            for tab in input_tables:
                # List all derived columns of the base table
                for col in tab.get_columns():
                    if col.get("operation").startswith("attr"):
                        continue
                    # Get its dependencies
                    deps = col.get_dependencies()
                    if self.id in deps:
                        continue  # Do NOT include in dependencies because it depends on this table

                    dependencies.append(col)

        elif operation.lower().startswith("prod"):
            pass

        elif operation.lower().startswith("filt"):
            base_table = input_tables[0]

            # Base table filter column
            columns = definition.get("columns")
            filter_column_name = columns[0]
            filter_column = base_table.get_column(filter_column_name)
            dependencies.append(filter_column)

        elif operation.lower().startswith("proj"):
            # Source table
            source_table_name = tables[0]
            source_table = input_tables[0]

            # Source table link column input keys (not the link column itself)
            link_column_name = definition.get("link")
            link_column = source_table.get_column(link_column_name)

            # Input columns of the link column
            link_column_ops = self.prosto.get_column_operations(source_table_name, link_column_name)
            link_column_op = link_column_ops[0]
            source_keys = link_column_op.get_columns()
            dependencies.extend(self.prosto.get_columns(source_table_name, source_keys))

        else:
            raise ValueError("Unknown operation type '{}' in the definition of table '{}'.".format(operation, self.id))

        return dependencies

    def evaluate(self) -> None:
        """Execute this operation by populating the output table. Only attribute columns are filled with values."""
        definition = self.definition
        operation = definition.get("operation", "UNKNOWN")

        input_length = definition.get("input_length", "UNKNOWN")  # value for row-based functions or table for column-based functions
        data_type = definition.get("data_type", "DataFrame")

        model = definition.get("model")

        outputs = definition.get("outputs")
        output_table_name = outputs[0]
        output_table = self.prosto.get_table(output_table_name)

        if operation.lower().startswith("noop"):
            new_data = None
        if operation.lower().startswith("popu"):

            if input_length == "row":
                new_data = self._evaluate_populate_row()
            elif input_length == "table":
                new_data = self._evaluate_populate_table()
            else:
                raise ValueError("Unknown input_type parameter '{}'.".format(input_length))

        elif operation.lower().startswith("prod"):
            new_data = self._evaluate_product()

        elif operation.lower().startswith("filt"):
            new_data = self._evaluate_filter()

        elif operation.lower().startswith("proj"):
            new_data = self._evaluate_project()

        else:
            raise ValueError("Unknown operation type '{}' in the definition of table '{}'.".format(operation, self.id))

        if new_data is not None:
            output_table.data.remove_all()
            output_table.data.add(new_data)

    def _evaluate_populate_row(self):
        """The function is applied to one row (from an input table) and generates a sub-table which will be appnded to the result."""
        definition = self.definition
        raise NotImplementedError("Row-based table population not supported.".format())

    def _evaluate_populate_table(self):
        """The result dataframe is genreated by the specified function using dataframe from input tables and model parameters."""
        definition = self.definition

        #
        # Stage 1. Resolve the function
        #
        func_name = definition.get("function")
        if not func_name:
            raise ValueError("Table function '{}' is not specified. Skip table definition.".format(func_name))

        func = resolve_full_name(func_name)
        if not func:
            raise ValueError("Cannot resolve user-defined function '{}'. Skip table definition.".format(func_name))

        #
        # Stage 2. Prepare input data
        #
        tables = self.get_tables()
        tables = self.prosto.get_tables(tables)
        if not tables: tables = []

        #
        # Stage 3. Prepare argument object to pass to the function as the second argument
        #
        model = definition.get("model")

        #
        # Stage 4. Apply function
        #
        out = None
        if len(tables) == 0:
            if model:
                out = func(**model)
            else:
                out = func()

        elif len(tables) == 1:  # Pass a single data frame
            if model:
                out = func(tables[0].get_data(), **model)
            else:
                out = func(tables[0].get_data())

        else:  # Pass a list of data frames
            if model:
                out = func([t.get_data() for t in tables], **model)
            else:
                out = func([t.get_data() for t in tables])

        #
        # Stage 5. Check that the result includes all declared attributes
        #
        attributes = definition.get("attributes", [])
        columns = out.columns.tolist()
        for att in attributes:
            if att not in columns:
                raise ValueError("Declared attribute '{}' is not in the populated table data.".format(att))

        #
        # Stage 6. Ensure that it has an integer index
        #
        out.reset_index(inplace=True, drop=True)

        return out

    def _evaluate_product(self):
        """The output table is a Cartesian product of the input tables with attributes pointing to the input records they are made of."""
        definition = self.definition

        outputs = definition.get("outputs")
        output_table = self.prosto.get_table(outputs[0])

        attributes = output_table.definition.get("attributes", [])

        #
        # Stage 1. Prepare input data
        #
        tables = self.get_tables()
        if not tables:
            raise ValueError("Table product operation must specify at least one table in the 'tables' field.".format())
        if len(tables) != len(attributes):
            raise ValueError("Number of input tables must be equal to the number of attributes in product table definition.".format())

        tables = self.prosto.get_tables(tables)
        table_datas = [x.get_data() for x in tables]

        #
        # Compute Cartesian product of all tables on their indexes
        #

        table_indexes = [x.index for x in table_datas]

        # Compute cartesian product
        index = pd.MultiIndex.from_product(iterables=table_indexes, names=attributes)

        out = pd.DataFrame(index=index)
        out.reset_index(inplace=True)

        return out

    def _evaluate_filter(self):
        """A new (filtered) table is generated by using a boolen column to select rows."""
        definition = self.definition

        outputs = definition.get("outputs")
        output_table = self.prosto.get_table(outputs[0])

        #
        # Stage 1. Prepare input data
        #
        tables = self.get_tables()
        if not tables:
            raise ValueError("Table filter operation must specify one base table in the 'tables' field.".format())
        tables = self.prosto.get_tables(tables)
        base_table = tables[0]

        base_table_data = base_table.get_data()

        #
        # Stage 2. Find filter column
        #
        columns = self.get_columns()
        if not columns:
            raise ValueError("Filter operation must specify a boolean column from the base table in the 'columns' field.".format())

        filter_column_name = columns[0]
        filter_column = base_table.get_column_data(filter_column_name)

        #
        # Stage 3. Find name of the super link-attribute which will point from this table to the base table
        #
        attributes = output_table.definition.get("attributes", [])
        if len(attributes) != 1:
            raise ValueError("Filter table must declare one attribute for linking to the base table.".format())
        super_attribute = attributes[0]

        #
        # Stage 4. Apply filter
        #
        out = base_table_data[filter_column]

        #
        # Stage 5. Convert base index into a "super" link attribute
        #
        out = pd.DataFrame(index=out.index)  # We need only index
        out.reset_index(inplace=True, drop=False)  # Convert index into a normal column
        out.rename(columns={"index": super_attribute}, inplace=True)  # Every new table has its own 0-based index

        return out

    def _evaluate_project(self):
        """Find unique combinations of the projected columns and store them as attributes in the populated table."""
        definition = self.definition

        outputs = definition.get("outputs")
        output_table = self.prosto.get_table(outputs[0])

        #
        # Stage 1. Prepare input data
        #
        tables = self.get_tables()
        if not tables:
            raise ValueError("Project operation must specify one input table in the 'tables' field".format())
        tables = self.prosto.get_tables(tables)

        source_table = tables[0]
        source_table_data = source_table.get_data()

        #
        # Stage 2. Find link column
        #
        link_column_name = definition.get("link")
        if not link_column_name:
            raise ValueError("Project operation must specify a link column from the input table in the 'function' field.".format())
        link_column = source_table.get_column(link_column_name)

        #
        # Stage 3. Find parameters of projection: source keys and target attribute names
        #

        # Find source table keys to be projected
        link_column_ops = self.prosto.get_column_operations(source_table.id, link_column.id)
        link_column_op = link_column_ops[0]
        source_keys = link_column_op.get_columns()
        if not all_columns_exist(source_keys, source_table_data):
            raise ValueError("Not all key columns available in the link column definition.".format())

        # Find this (target) table attributes to be created
        attributes = output_table.definition.get("attributes", [])
        if len(attributes) != len(source_keys):
            raise ValueError("Project table must declare one attribute for each input of the corresponding link column.".format())

        # Link column can define its own target keys. We do not use them but want to check the validity because the link evaluation can fail later.
        linked_inputs = link_column_op.definition.get("linked_inputs", [])
        if len(linked_inputs) > 0 and set(attributes) != set(linked_inputs):
            raise ValueError("Attributes of the project table are different from the 'linked_inputs' of the corresonding link column.".format())

        #
        # Stage 4. Produce all unique combinations of the input columns
        #
        """
        INFO:
        df_new = df.drop_duplicates(subset=["C1", "C2", "C3"])  # Drop duplicates
        a_df = df.drop_duplicates(["col1", "col2"])[["col1", "col2"]]
        df = df.groupby(by=["C1", "C2", "C3"], as_index=False).first()  # Using groupby
        np.unique(df[["col1", "col2"]], axis=0)  # Not for object data (error for object types)
        """
        out = source_table.data.get_df().drop_duplicates(subset=source_keys)  # Really do projection

        #
        # Stage 5. Index and renamings
        #
        out = out[source_keys]  # Leave only project columns (de-duplicate will return *all* source columns)

        # Rename to attribute names (de-duplicate will return source table columns)
        rename_dict = dict(zip(source_keys, attributes))  # # source_keys (keys) -> attribute_names (values)
        out.rename(columns=rename_dict, inplace=True)

        out.reset_index(drop=True, inplace=True)  # Every new table has its own 0-based index

        return out


if __name__ == "__main__":
    pass
