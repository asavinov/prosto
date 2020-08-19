from typing import Union, Any, List, Set, Dict, Tuple, Optional
import json
import math

from prosto.utils import *
from prosto.resolve import *

import prosto as pr  # To resolve circular imports
from prosto.Prosto import *
from prosto.Table import *
from prosto.Column import *
from prosto.Operation import *


class ColumnOperation(Operation):
    """The class represents one column operation."""

    def __init__(self, prosto, definition):
        super(ColumnOperation, self).__init__(prosto, definition)

    def get_dependencies_names(self) -> dict:
        """
        Get all dependencies represented by names like table names and column names as they are specified in the definition.
        These names are not resolved and might not be in the list of schema objects yet (like attributes or column paths).
        :return: dict with table names as keys and lists of columns as values (empty in the case of only table as a dependency)
        """

        definition = self.definition
        operation = definition.get("operation", "UNKNOWN")

        output_table_name = definition.get("table")

        outputs = self.get_outputs()
        output_column_name = outputs[0]

        columns = self.get_columns()

        dependencies = {}

        # All derived columns depend on their table
        dependencies[output_table_name] = []

        #
        # Collect operation-specific dependencies
        #

        if operation.lower().startswith("comp"):
            # Input column objects for which we need to find definitions
            dependencies[output_table_name].extend(columns)

        elif operation.lower().startswith("calc"):
            # Input column objects for which we need to find definitions
            dependencies[output_table_name].extend(columns)

        elif operation.lower().startswith("link"):
            # Input (fact table) columns or column paths have to be evaluated
            dependencies[output_table_name].extend(columns)

            # Target (linked) table has to be populated
            linked_table_name = self.prosto.get_type_table(output_table_name, output_column_name)
            dependencies[linked_table_name] = []

            # Target columns have to be evaluated in order to contain values. However, they are supposed to be attributes and hence they will be set during population
            linked_columns = definition.get("linked_columns", [])
            dependencies[linked_table_name].extend(linked_columns)

        elif operation.lower().startswith("merg"):
            # Link column (first segment) has to be evaluated
            link_column_name = columns[0]
            dependencies[output_table_name].append(link_column_name)

            # Linked table has to be populated. (Yet, it will be added to dependency by the link column.)
            linked_table_name = self.prosto.get_type_table(output_table_name, link_column_name)
            dependencies[linked_table_name] = []

            # Linked column path (tail) in the linked table has to exist (recursion)
            linked_column_name = columns[1]
            dependencies[linked_table_name].append(linked_column_name)

        elif operation.lower().startswith("roll"):
            # Columns to be aggregated
            dependencies[output_table_name].extend(columns)

            # Link (group) column
            link_column_name = definition.get("link")
            if link_column_name:
                dependencies[output_table_name].append(link_column_name)

            # Linked table (if any) has to be populated. (Yet, it will be added to dependency by the link column.)
            if link_column_name:
                linked_table_name = self.prosto.get_type_table(output_table_name, link_column_name)
                if linked_table_name:
                    dependencies[linked_table_name] = []

        elif operation.lower().startswith("aggr"):
            # The fact table has to be already populated
            tables = self.get_tables()
            source_table_name = tables[0]
            dependencies[source_table_name] = []

            # Measure columns of the fact table
            dependencies[source_table_name].extend(columns)

            # Link (group) column
            link_column_name = definition.get("link")
            dependencies[source_table_name].append(link_column_name)

        elif operation.lower().startswith("disc"):
            # Input column objects for which we need to find definitions
            dependencies[output_table_name].extend(columns)

        else:
            raise ValueError("Unknown operation type '{}' in the definition of column '{}'.".format(operation, self.id))

        return dependencies

    def get_dependency_objects(self) -> List[Union[Table, Column]]:
        """
        Get dependencies as a list of objects of Table or Column resolved from the corresponding names.
        Note that not all names can be resolved to objects, for example, attributes, column paths or expressions.
        """
        deps = self.get_dependencies_names()

        dependencies = []

        # Tables
        for table_name in deps.keys():
            # Resolve this table name into a table object
            table = self.prosto.get_table(table_name)
            dependencies.append(table)

        # Columns
        for table_name, column_names in deps.items():
            for column_name in column_names:
                # Resolve column name
                column = self.prosto.get_column(table_name, column_name)
                if column:
                    dependencies.append(column)
                else:
                    pass  # It could be an attribute or column path

        return dependencies

    def evaluate(self) -> None:
        """
        Execute this column operation and evaluate the output column(s).

        A generic sequence of operations:
        - prepare the input slice by selecting input columns and input rows
        - convert the selected slice to the necessary data format expected by UDF
        - process input data by calling UDF or operation and returning some result
        - convert the result to our standard format
        - impose the result to our current data by overwriting the output columns and output values

        Notes:
        - There are two types of definitions: relying on UDFs (calc, roll, aggr), and not using UDFs (link, merge)
        - There are UDFs of two types: value or row based (returning a value or row), and column or table based (returning a whole column or table)
        """
        definition = self.definition
        operation = definition.get("operation", "UNKNOWN")

        input_length = definition.get("input_length", "UNKNOWN")  # value for value-based functions or column for column-based functions
        data_type = definition.get("data_type", "Series")

        model = definition.get("model")

        output_table_name = definition.get("table")
        output_table = self.prosto.get_table(output_table_name)

        outputs = self.get_outputs()
        output_column_name = outputs[0]
        output_column = self.prosto.get_column(output_table_name, output_column_name)

        data = output_table.get_data()

        #
        # Operations without UDF
        #

        # Link columns use their own definition format different from computational (functional) definitions
        if operation.lower().startswith("link"):
            out = self._evaluate_link()

            self._impose_output_columns(out)

            return

        # Compose columns use their own definition format different from computational (functional) definitions
        if operation.lower().startswith("merg"):
            out = self._evaluate_merge()

            self._impose_output_columns(out)

            return

        # Discretize column using some logic of partitioning represented in the model
        if operation.lower().startswith("disc"):
            # Determine input columns
            columns = self.get_columns()
            columns = get_columns(columns, data)
            if columns is None:
                raise ValueError("Error reading column list. Skip column definition.")

            # Validation: check if all explicitly specified columns available
            if not all_columns_exist(columns, data):
                raise ValueError("Not all input columns available. Skip column definition.".format())

            # Slice input according to the change status
            if self.prosto.incremental:
                data = output_table.data.get_added_slice(columns)
                range = output_table.data.added_range
            else:
                data = output_table.data.get_full_slice(columns)
                range = output_table.data.id_range()


            out = self._evaluate_discretize(data, model)

            self._impose_output_columns(out, range)

            return

        #
        # Operations with UDF
        #

        func_name = definition.get("function")
        if not func_name:
            raise ValueError("Column function '{}' is not specified. Skip column definition.".format(func_name))

        func = resolve_full_name(func_name)
        if not func:
            raise ValueError("Cannot resolve user-defined function '{}'. Skip column definition.".format(func_name))

        if operation.lower().startswith("comp") or operation.lower().startswith("calc"):
            # Determine input columns
            columns = self.get_columns()
            columns = get_columns(columns, data)
            if columns is None:
                raise ValueError("Error reading column list. Skip column definition.")

            # Validation: check if all explicitly specified columns available
            if not all_columns_exist(columns, data):
                raise ValueError("Not all input columns available. Skip column definition.".format())

            # Slice input according to the change status
            if self.prosto.incremental:
                data = output_table.data.get_added_slice(columns)
                range = output_table.data.added_range
            else:
                data = output_table.data.get_full_slice(columns)
                range = output_table.data.id_range()

            if operation.lower().startswith("comp"):  # Equivalently: input_length == "column"
                out = self._evaluate_compute(func, data, data_type, model)
            elif operation.lower().startswith("calc"):  # Equivalently: input_length == "value"
                out = self._evaluate_calculate(func, data, data_type, model)
            else:
                raise ValueError("Unknown input_type parameter '{}'.".format(input_length))

        elif operation.lower().startswith("roll"):
            # Determine input columns
            columns = self.get_columns()
            columns = get_columns(columns, data)
            if columns is None:
                raise ValueError("Error reading input column list. Skip column definition.")

            # Validation: check if all explicitly specified columns available
            if not all_columns_exist(columns, data):
                raise ValueError("Not all input columns available. Skip column definition.".format())

            # It exists only for rolling aggregation with grouping
            link_column_name = definition.get("link")

            # Slice input according to the change status (incremental not implemented)
            data = output_table.data.get_full_slice(columns)
            range = output_table.data.id_range()

            if input_length == "value":
                raise NotImplementedError("Accumulation is not implemented.".format())
            elif input_length == "column":
                gb = output_table._get_or_create_groupby(link_column_name) if link_column_name else None
                out = self._evaluate_roll(func, gb, data, data_type, model)
            else:
                raise ValueError("Unknown input_type parameter '{}'.".format(input_length))

        elif operation.lower().startswith("aggr"):
            #
            # Get parameters
            #
            tables = self.get_tables()
            source_table_name = tables[0]
            source_table = self.prosto.get_table(source_table_name)
            if source_table is None:
                raise ValueError("Cannot find the fact table '{}'.".format(source_table_name))

            link_column_name = definition.get("link")
            link_column = source_table.get_column(link_column_name)
            if link_column is None:
                raise ValueError("Cannot find the link column '{}'.".format(link_column_name))

            data = source_table.get_data()  # Data (to be processed) is a (source) table which is different from the output table

            # Determine input columns
            columns = self.get_columns()
            columns = get_columns(columns, data)
            if columns is None:
                raise ValueError("Error reading input column list. Skip column definition.")

            # Validation: check if all explicitly specified columns available
            if not all_columns_exist(columns, data):
                raise ValueError("Not all input columns available. Skip column definition.".format())

            data = data[columns]  # Select only the specified *input* columns

            data_type = definition.get("data_type")

            # No incremental. Select full *output* range
            range = output_table.data.id_range()

            if input_length == "value":
                raise NotImplementedError("Accumulation is not implemented.".format())
            elif input_length == "column":
                gb = source_table._get_or_create_groupby(link_column_name)
                out = self._evaluate_aggregate(func, gb, data, data_type, model)
            else:
                raise ValueError("Unknown input_type parameter '{}'.".format(input_length))

        else:
            raise ValueError("Unknown operation type '{}' in the definition of column '{}'.".format(operation, self.id))

        #
        # Append the newly generated column(s) to this table
        #
        self._impose_output_columns(out, range)

    def _evaluate_calculate(self, func, data, data_type, model):
        """Calculate column. Apply function to each row of the table."""

        #
        # Single input: Apply to a series. UDF will get single value
        #
        if len(data.columns) == 1:
            data_arg = data[data.columns[0]]  # Series

            # Determine format/type of representation
            if data_type == "ndarray":
                data_arg = data_arg.values

            #
            # Call UDF depending on the necessary model parameter
            #
            if model is None:
                out = pd.Series.apply(data_arg, func)  # Do not pass model to the function
            elif isinstance(model, (list, tuple)):
                out = pd.Series.apply(data_arg, func, args=model)  # Model as positional arguments
            elif isinstance(model, dict):
                # Pass model by flattening dict (alternative: arbitrary Python object as positional or key argument). UDF has to declare the expected arguments
                out = pd.Series.apply(data_arg, func, **model)  # Model as keyword arguments
            else:
                out = pd.Series.apply(data_arg, func, args=(model,))  # Model as an arbitrary object

        #
        # Multiple inputs: Apply to a frame. UDF will get a row of values
        #
        else:
            # Notes:
            # - UDF expects one row as a data input (raw=True - ndarry, raw=False - Series)
            # - model (arguments) cannot be None, so we need to guarantee that we do not pass None

            # Determine format/type of representation
            if data_type == "ndarray":
                data_arg = data.values
                raw_arg = True
            else:
                data_arg = data
                raw_arg = False

            #
            # Call UDF depending on the necessary model parameter
            #
            if model is None:
                out = pd.DataFrame.apply(data_arg, func, axis=1, raw=raw_arg)  # Do not pass model to the function
            elif isinstance(model, (list, tuple)):
                out = pd.DataFrame.apply(data_arg, func, axis=1, raw=raw_arg, args=model)  # Model as positional arguments
            elif isinstance(model, dict):
                out = pd.DataFrame.apply(data_arg, func, axis=1, raw=raw_arg, **model)  # Model as keyword arguments
            else:
                out = pd.DataFrame.apply(data_arg, func, axis=1, raw=raw_arg, args=(model,))  # Model as an arbitrary object

        return out

    def _evaluate_compute(self, func, data, data_type, model):
        """Calculate column. Apply function to all inputs and return calculated column(s)."""
        #
        # Determine data argument shape and format/type
        #
        if len(data.columns) == 1:
            data_arg = data[data.columns[0]]  # Series
        else:
            data_arg = data  # DataFrame

        # Determine format/type of representation
        if data_type == "ndarray":
            data_arg = data_arg.values

        #
        # Call UDF depending on the necessary model parameter
        #
        if model is None:
            out = func(data_arg)  # No model
        elif isinstance(model, (list, tuple)):
            out = func(data_arg, *model)  # Model as positional arguments
        elif isinstance(model, dict):
            out = func(data_arg, **model)  # Model as keyword arguments
        else:
            out = func(data_arg, model)  # Model as an arbitrary object

        return out

    def _evaluate_link(self):
        """Link column. Output column will store ids (indexes) of the target table rows."""
        definition = self.definition

        #
        # Read and validate parameters
        #

        main_table_name = definition.get("table")
        main_table = self.prosto.get_table(main_table_name)

        outputs = self.get_outputs()
        column_name = outputs[0]
        output_column = self.prosto.get_column(main_table_name, column_name)

        main_keys = self.get_columns()
        if not all_columns_exist(main_keys, main_table.get_data()):
            raise ValueError("Not all key columns available in the link column definition.".format())

        linked_table_name = self.prosto.get_type_table(main_table_name, column_name)
        linked_table = self.prosto.get_table(linked_table_name)
        if not linked_table:
            raise ValueError("Linked table '{}' cannot be found in the link column definition.".format(linked_table))

        linked_columns = definition.get("linked_columns", [])
        if len(linked_columns) == 0:
            linked_columns = linked_table.definition.get("attributes", [])  # By default (e.g., for projection), we link to target table attributes
        if not all_columns_exist(linked_columns, linked_table.get_data()):
            raise ValueError("Not all linked key columns available in the link column definition.".format())

        #
        # 1. In the target (linked) table, convert its index into a normal column
        # The reason is that we can only merge normal columns and not index.
        # The values of this index column will be copied to our new link column and hence will reference the linked rows
        #
        index_column_name = "__row_id__" # It could be "id", "index" or whatever other convention
        linked_table.get_data()[index_column_name] = linked_table.get_data().index
        # df.reset_index(inplace=True).set_index("index", drop=False, inplace=True)  ä Alternative 1: reset will convert index to column, and then again create index
        # df = df.rename_axis("index1").reset_index() # Alternative 2: New index1 column will be created

        #
        # 2. Create left join on the specified keys
        #

        linked_prefix = column_name + pr.Prosto.column_path_separator  # It will be prepended to each linked (secondary) column name

        out_df = pd.merge(
            main_table.get_data(),  # This table
            linked_table.get_data().rename(columns=lambda x: linked_prefix + x, inplace=False),  # Target table to link to. We rename columns (not in place - the original frame preserves column names)
            how="left",  # This (main) table is not changed - we attach target records
            left_on=main_keys,  # List of main table key columns
            right_on= [linked_prefix + x for x in linked_columns],  # List of target table key columns. Note that we renamed them above so we use modified names
            left_index=False,
            right_index=False,
            #suffixes=("", linked_suffix),  # We do not use suffixes because they cannot be enforced (they are used only in the case of equal column names)
            sort=False  # Sorting decreases performance
        )

        # We do not need this column anymore - it was merged (copied) to the result as a link column
        linked_table.get_data().drop(columns=[index_column_name], inplace=True)

        #
        # 3. Rename according to our convention and store the result
        #

        # Rename our link column by using only specified column name
        out_df.rename({column_name + pr.Prosto.column_path_separator + index_column_name: column_name}, axis="columns", inplace=True)

        out = out_df[column_name]  # We need only one column from the result data frame

        return out

    def _evaluate_merge(self):
        """Merge column. Materialize a complex column path which is sequence of link columns ending with some target column."""
        definition = self.definition

        #
        # Read and validate parameters
        #
        output_table_name = definition.get("table")
        output_table = self.prosto.get_table(output_table_name)
        output_table_data = output_table.get_data()

        outputs = self.get_outputs()
        output_column_name = outputs[0]
        output_column = self.prosto.get_column(output_table_name, output_column_name)

        columns = self.get_columns()
        link_column_path = ""  # Column path composed of several separated column segment names
        df = output_table_data
        main_table_name = output_table_name
        main_table = output_table
        for i, link_column_name in enumerate(columns):
            #
            # Build link column path by appending a new column segment name
            # Note that this path is a name of the column in the data frame and it does not have a definition
            #
            if i == 0:
                link_column_path = link_column_name
            else:
                link_column_path += pr.Prosto.column_path_separator + link_column_name

            #
            # Stop condition if there is no further segment to merge.
            #
            if i == len(columns) - 1:
                # Rename the last merged column path to the desired column name as specified in the definition
                df.rename(columns={link_column_path: output_column_name}, inplace=True)
                out = df[output_column_name]
                break

            #
            # Find link column definition
            #
            link_column = main_table.get_column(link_column_name)

            #
            # Find the linked table
            #
            linked_table_name = self.prosto.get_type_table(main_table_name, link_column_name)
            linked_table = self.prosto.get_table(linked_table_name)
            linked_table_data = linked_table.get_data()

            #
            # Find the target linked column in the linked table
            #
            linked_column_name = columns[i+1]
            #linked_column = linked_table.get_column(linked_column_name)

            #
            # Do merge
            #
            df = self._merge_two_columns(
                df,
                link_column_path,
                linked_table_data,
                linked_column_name
            )

            # Iterate
            main_table_name = linked_table_name
            main_table = linked_table

        return out

    def _merge_two_columns(self, source_df, link_column_name, target_df, linked_column_name):
        """Given source and target tables with link and linked columns, return a new column with the source table index and target column values."""

        linked_prefix = link_column_name + pr.Prosto.column_path_separator  # It will prepended to each linked (secondary) column name

        out_df = pd.merge(
            source_df[[link_column_name]],
            target_df[[linked_column_name]].rename(columns=lambda x: linked_prefix + x, inplace=False),  # We rename target columns to avoid conflicts (not in place - the original frame preserves column names)
            how="left",  # This (source) table is not changed - we want to attach target records
            left_on=link_column_name,  # Its values are row ids stored in the linked table index
            right_on=None,
            left_index=False,
            right_index=True,  # Use index because link column stores index values
            #suffixes=("", linked_suffix),  # We do not use suffixes because they cannot be enforced (they are used only in the case of equal column names)
            sort=False  # Sorting decreases performance
        )
        # Here we get linked column names like "Prefix::OriginalName"

        return out_df

    def _evaluate_discretize(self, data, model):
        """Discretize column. Apply discretization function to each row of the table."""
        definition = self.definition

        # Currently we discretize only one numeric column

        #
        # Single input: Apply to a series. UDF will get single value
        #
        if isinstance(data, pd.Series) or ((isinstance(data, pd.DataFrame) and len(data.columns) == 1)):
            if isinstance(data, pd.DataFrame):
                ser = data[data.columns[0]]
            else:
                ser = data
        else:
            raise ValueError("Discretize expects only one column as input")

        # Discretization functions
        # Model example: {
        #   "origin/base/ancor": 1, (default is 0) - value the steps are started from (also negative). it is always label no 0 (but can represent left/negative or right/positive interval).
        #   "step/freq/rule": 10, - length of one whole interval (unit of the raster)
        #   "label/label_end/border": "left/right" (return left or right border as id, default is left),
        #   "closed": "left/right" (default left),
        #   "label_value": "step/border" (default step_no, interval_no) - return intervla number or border value (note that returning float value is a bad idea because floats are bad representatives for discrete groups, also step/interval_no are continuous)
        #   }
        def disc_numeric_fn(value, **model):
            # How many whole steps from origin till this point (including or excluding) and what is the remainder
            # How many whole (integer) steps are from origin to this value (in both directions)
            #   What is origin - label number 0? If so, then we determine which, left or righ interval it represents with left-right borders, and it is a basis of further computations.
            #   It is (value - origin) // step OR float division and then round by removing after dot (which is remainder)
            #   int() rounds down so 1.9 will be 1,
            #   math.floor(-0.5) = -1, 1.2 -> 1,

            # ----0---------1---------2---------3--- label_no
            #    )[        )[        )[        )[ closed left - either left or right label
            #     ](        ](        ](        ]( closed right - either left or right label

            # Get parameters
            origin = model.get("origin", 0)
            step = model.get("step", 1)
            label = model.get("label", "left")
            closed = model.get("closed", "left")
            label_value = model.get("label_value", "interval")

            steps = (value - origin) / step
            left_border_no = math.floor(steps)  # floor: the largest integer less than or equal to x, floor(-0.5) = -1
            right_border_no = math.ceil(steps)  # ceil: the smallest integer greater than or equal to x
            # Alternatively, use math.modf(x) - Return the fractional and integer parts of x
            # Alternatively, use int()

            #
            # Determine borders of the interval the value belongs to
            #
            if left_border_no == right_border_no:  # Value is exactly on the border - interval is not known (either left or right)
                if closed == "left":  # Interval on the right
                    right_border_no += 1
                else:  # Interval on the left
                    left_border_no -= 1

            #
            # Determine label for this interval
            #
            if label == "left":
                label_no = left_border_no
            else:
                label_no = right_border_no

            if label_value == "interval":
                return label_no
            else:
                return label_no * step

        if model is None:
            raise ValueError("Discretize expects non-empty model.")
        elif isinstance(model, (list, tuple)):
            out = pd.Series.apply(ser, disc_numeric_fn, args=model)  # Model as positional arguments
        elif isinstance(model, dict):
            # Pass model by flattening dict (alternative: arbitrary Python object as positional or key argument). UDF has to declare the expected arguments
            out = pd.Series.apply(ser, disc_numeric_fn, **model)  # Model as keyword arguments
        else:
            out = pd.Series.apply(ser, disc_numeric_fn, args=(model,))  # Model as an arbitrary object

        return out

    def _evaluate_roll(self, func, gb, data, data_type, model):
        """Roll column. Apply aggregate function to each window defined on this same table for every record."""
        definition = self.definition

        #
        # Determine window size. The window parameter can be string, number or object (many arguments for rolling object)
        #
        window = definition.get("window")
        window_size = int(window)
        rolling_args = {"window": window_size}
        # TODO: try/catch with exception if cannot get window size

        #
        # Single input. UDF will get a window sub-series as a data argument
        #
        if len(data.columns) == 1:

            in_column = data.columns.to_list()[0]

            if data_type == "ndarray":
                raw_arg = True
            else:
                raw_arg = False

            if gb is None:  # single column without link/grouping

                # Create a rolling object with windowing (row-based windowing independent of the number of columns)
                rl = pd.DataFrame.rolling(data, **rolling_args)  # as_index=False - aggregations will produce flat DataFrame instead of Series with index

                # Invoke depending on the model type
                if model is None:
                    out = rl[in_column].apply(func, raw=raw_arg)  # No model
                elif isinstance(model, (list, tuple)):
                    out = rl[in_column].apply(func, raw=raw_arg, args=model)  # Model as positional arguments
                elif isinstance(model, dict):
                    out = rl[in_column].apply(func, raw=raw_arg, **model)  # Model as keyword arguments
                else:
                    out = rl[in_column].apply(func, raw=raw_arg, args=(model,))  # Model as an arbitrary object

            else:  # single column with link/grouping

                def groll_fn(g):
                    rl = pd.DataFrame.rolling(g, **rolling_args)
                    out = rl.apply(func, **model)  # TODO: Pass model to the function
                    return out

                # Apply rolling function to each group
                # NOTE: "Groupby preserves the order of rows within each group" and therefore rolling should be correct when applied to individual groups
                out = gb[in_column].apply(groll_fn)

                # The result is a series with aggregated values but it has MultiIndex: first level group, second level original id. Therefore, we cannot directly add it to the original frame
                # Example for two devices and 6 rows: MultiIndex(levels=[[0, 1], [0, 1, 2, 3, 4, 5]], labels=[[0, 0, 0, 1, 1, 1], [0, 2, 4, 1, 3, 5]])
                # Remove first level of index (group numbers). The labels of the second level will be then converted to a normal index with original ids corresponding to the frame index
                out.reset_index(level=0, drop=True, inplace=True)

        #
        # Multiple inputs. UDF will get a window sub-dataframe as a data argument
        #
        else:
            #
            # Problem (of pandas): rolling apply passes only a series to UDF - we are not able to pass a frame (with multiple columns)
            # Workaround: create a new temporary data frame with all row ids, create a rolling object over it, apply auxiliary function to it, it will get a window/group of row ids which can be then used to access this window rows from the main data frame:
            # Link: https://stackoverflow.com/questions/45928761/rolling-pca-on-pandas-dataframe
            #

            # Auxiliary function
            # It gets a window as a series of row ids and an aggregate function
            # It will select a sub-dataframe using these ids and pass it to UDF for aggregation and return the value
            # Note that it selects data from a well known variable and uses a model defined outside
            def window_fn(ids, user_fn):
                df_window = data.loc[ids]  # Select rows of the window with the necessary ids

                # Determine format/type of representation
                if data_type == "ndarray":
                    data_arg = df_window.values
                else:
                    data_arg = df_window

                # Apply UDF to this window by invoking depending on the model type
                if model is None:
                    out = user_fn(data_arg)  # No model
                elif isinstance(model, (list, tuple)):
                    out = user_fn(data_arg, *model)  # Model as positional arguments
                elif isinstance(model, dict):
                    out = user_fn(data_arg, **model)  # Model as keyword arguments
                else:
                    out = user_fn(data_arg, model)  # Model as an arbitrary object

                return out

            if gb is None:  # multiple columns without link/grouping

                # Generate sequential row ids like 0,1,2,...
                # NOTE: Why not to re-use existing integer index?
                # NOTE: We should add a datetime column if it is used in the window definition
                df_idx = pd.DataFrame(data=data.index, index=data.index)
                #df_idx = pd.DataFrame(np.arange(data.shape[0]))

                # Create rolling object from ids-frame
                # NOTE: We can use only row-based windows (not custom column based windows like timestamps with duration as a window parameter)
                rl_idx = df_idx.rolling(**rolling_args)

                # Apply UDF for rolling aggregation
                out = rl_idx.apply(lambda x: window_fn(x, func), raw=False)

            else:  # multiple columns with link/grouping

                out = pd.Series(index=data.index)
                out.values[:] = None  # We need to assign default value which should be a parameter of column or operation

                # Perform rolling aggregation for each group independently in the loop and then merge all the results
                # The body is logically equivalent to rolling aggregation without grouping (see above)
                for n, df_g in gb:
                    # g is a data frame with all records belonging to one group

                    # Generate row ids for this group
                    df_idx = pd.DataFrame(data=df_g.index, index=df_g.index)

                    # Create rolling object for these ids (of this group only)
                    rl_idx = df_idx.rolling(**rolling_args)

                    # Apply UDF for rolling aggregation by sending window ids, all data with all ids and function
                    # Produce output for this group only (for indexes of this group)
                    out_g = rl_idx.apply(lambda x: window_fn(x, func), raw=False)

                    # Append results of aggregation for this group to the final result by index-based imposing (update)
                    out.update(out_g[0])

        return out

    def _evaluate_aggregate(self, func, gb, data, data_type, model):
        """Link (group) column. Apply aggregate function to each group of records of the fact table."""
        definition = self.definition

        #
        # Special case: no input columns (or function is size()
        #
        if len(data.columns) == 0:
            out = gb.size()

        #
        # Single input. UDF will get a window sub-series as a data argument
        #
        elif len(data.columns) == 1:
            in_column = data.columns.to_list()[0]
            # Invoke depending on the model type
            if model is None:
                out = gb[in_column].agg(func)  # No model
            elif isinstance(model, (list, tuple)):
                out = gb[in_column].agg(func, args=model)  # Model as positional arguments
            elif isinstance(model, dict):
                out = gb[in_column].agg(func, **model)  # Model as keyword arguments
            else:
                out = gb[in_column].agg(func, args=(model,))  # Model as an arbitrary object

        #
        # Multiple inputs. UDF will get a window sub-dataframe as a data argument
        #
        else:
            # Invoke depending on the model type
            if model is None:
                out = gb.apply(func)  # No model
            elif isinstance(model, (list, tuple)):
                out = gb.apply(func, args=model)  # Model as positional arguments
            elif isinstance(model, dict):
                out = gb.apply(func, **model)  # Model as keyword arguments
            else:
                out = gb.apply(func, args=(model,))  # Model as an arbitrary object

        return out

    def _impose_output_columns(self, out, range=None):
        """
        Append the specified column(s) to the data frame of the output table.
        This function will impose the input data frame onto the existing data.
        It will overwrite existing values with the same ids and columns as in input data frame
        Other values in this table which are not present in the input data frame will be not changed.
        None range means full id range.
        """
        definition = self.definition

        outputs = self.get_outputs()
        output_table_name = definition.get("table")
        output_table = self.prosto.get_table(output_table_name)

        fillna_value = definition.get("fillna_value")

        #
        # Change format to dataframe
        #
        if isinstance(out, pd.DataFrame):
            pass
        elif isinstance(out, pd.Series):
            out = pd.DataFrame(out)  # Series name will be column name
        elif isinstance(out, (list,tuple)):
            # TODO: Convert a list of series or data frames into one data frame. They all have to have same index.
            raise NotImplementedError("List (of series) as a result of evaluation is currently not supported.".format())
        elif isinstance(out, np.ndarray):
            # TODO: Convert ndarray into dataframe. Main problem is that we need to know the index and then assume that the values in ndarray are sequential.
            raise NotImplementedError("NumPy array as a result of evaluation is currently not supported.".format())

        #
        # Assign (custom) column names
        #
        if len(out.columns) > len(outputs):
            raise ValueError("Operation returned {} columns, which is more than specified in its definition for its output.".format(len(out.columns)))

        if len(out.columns) < len(outputs):
            raise ValueError("Operation returned {} columns, which is less than specified in its definition for its output.".format(len(out.columns)))

        out.columns = outputs[0:len(out.columns)]

        #
        # Write the result to the data by overwriting cells
        #
        output_table.data.set_column_values_for_range(out, range, fillna_value)


if __name__ == "__main__":
    pass
