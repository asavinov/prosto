import json

from prosto.utils import *

from prosto.Schema import *
from prosto.Table import *
from prosto.Column import *
from prosto.Operation import *

import logging
log = logging.getLogger('prosto.operation')


class ColumnOperation(Operation):
    """The class represents one column operation."""

    def __init__(self, schema, definition):
        super(ColumnOperation, self).__init__(schema, definition)

    def get_dependencies(self):
        """Get tables and columns this column depends upon."""
        definition = self.definition
        operation = definition.get('operation', 'UNKNOWN')

        output_table_name = definition.get('table')
        output_table = self.schema.get_table(output_table_name)

        outputs = self.get_outputs()
        output_column_name = outputs[0]
        output_column = self.schema.get_column(output_table_name, output_column_name)

        columns = self.get_columns()
        input_columns = self.schema.get_columns(output_table_name, columns)

        dependencies = []

        # All derived columns depend on their table
        dependencies.append(output_table)

        if operation.lower().startswith('calc'):
            # Input column objects for which we need to find definitions
            dependencies.extend(input_columns)

        elif operation.lower().startswith('link'):
            # Target (linked) table has to be populated
            linked_table_name = output_column.definition.get('type', '')
            linked_table = self.schema.get_table(linked_table_name)
            dependencies.append(linked_table)

            # Input (fact table) columns or column paths have to be evaluated
            dependencies.extend(input_columns)

            # Target columns have to be evaluated in order to contain values. However, they are supposed to be attributes and hence they will be set during population.
            linked_columns = definition.get('linked_columns', [])
            dependencies.extend(self.schema.get_columns(linked_table_name, linked_columns))

        elif operation.lower().startswith('merg'):
            # Link column (first segment) has to be evaluated
            link_column_name = next(iter(columns), None)
            link_column = self.schema.get_column(output_table_name, link_column_name)
            dependencies.append(link_column)

            # Linked column path (tail) in the linked table has to exist (recursion)
            linked_table_name = link_column.definition.get('type')
            linked_table = self.schema.get_table(linked_table_name)
            linked_column_name = columns[1]

            linked_column = self.schema.get_column(linked_table_name, linked_column_name)
            if linked_column:  # A linked column might not have a definition, e.g., an attribute
                dependencies.append(linked_column)
            # Here we assume that the tail dependencies will be retrieved separately.
            # Alternatively, we could retrieve them here using recursion

            # Lined table has to be populated. (Yet, it will be added to dependency by the link column.)
            dependencies.append(linked_table)

        elif operation.lower().startswith('roll'):
            # Columns to be aggregated
            dependencies.extend(input_columns)

        elif operation.lower().startswith('group'):
            # The fact table has to be already populated
            tables = definition.get('tables')
            source_table_name = tables[0]
            source_table = self.schema.get_table(source_table_name)
            dependencies.append(source_table)

            # Group column
            link_column_name = definition.get('link')
            link_column = source_table.get_column(link_column_name)
            dependencies.append(link_column)

            # Measure columns
            dependencies.extend(self.schema.get_columns(source_table_name, columns))

        else:
            return []

        return dependencies

    def evaluate(self):
        """Execute this column operation and evaluate the output column(s)."""
        definition = self.definition
        operation = definition.get('operation', 'UNKNOWN')

        input_length = definition.get('input_length', 'UNKNOWN')  # value for value-based functions or column for column-based functions
        data_type = definition.get('data_type', 'Series')

        model = definition.get('model')

        output_table_name = definition.get('table')
        output_table = self.schema.get_table(output_table_name)

        outputs = definition.get('outputs')
        output_column_name = outputs[0]
        output_column = self.schema.get_column(output_table_name, output_column_name)

        data = output_table.get_data()

        log.info(f"---> Start evaluating '{operation}' column '{self.id}'.")

        #
        # Operations without UDF
        #

        # Link columns use their own definition schema different from compuational (functional) definitions
        if operation.lower().startswith('link'):
            out = self._evaluate_link()

            self._append_output_columns(out)
            return

        # Compose columns use their own definition schema different from computaional (functional) definitions
        if operation.lower().startswith('merg'):
            out = self._evaluate_merge()

            self._append_output_columns(out)
            return

        #
        # Operations with UDF
        #

        func_name = definition.get('function')
        if not func_name:
            log.error(f"Column function '{func_name}' is not specified. Skip column definition.")
            return

        func = resolve_full_name(func_name)
        if not func:
            log.error(f"Cannot resolve user-defined function '{func_name}'. Skip column definition.")
            return

        if operation.lower().startswith('calc'):
            #
            # Prepare input data for the function
            #
            columns = self.get_columns()
            columns = get_columns(columns, data)
            if columns is None:
                log.error("Error reading column list. Skip column definition.")
                return

            # Validation: check if all explicitly specified columns available
            if not all_columns_exist(columns, data):
                log.error("Not all input columns available. Skip column definition.".format())
                return

            data = data[columns]  # Select only the specified input columns

            if input_length == 'value':
                out = self._evaluate_calc_value(func, data, data_type, model)
            elif input_length == 'column':
                out = self._evaluate_calc_column(func, data, data_type, model)
            else:
                log.error(f"Unknown input_type parameter '{input_length}'.")
                return

        elif operation.lower().startswith('roll'):

            if input_length == 'value':
                log.error(f"Accumulation is not implemented.")
                return
            elif input_length == 'column':
                out = self._evaluate_roll_column(func, data, data_type, model)
            else:
                log.error(f"Unknown input_type parameter '{input_length}'.")
                return

        elif operation.lower().startswith('group'):

            if input_length == 'value':
                log.error(f"Accumulation is not implemented.")
                return
            elif input_length == 'column':
                out = self._evaluate_group_column(func, model)
            else:
                log.error(f"Unknown input_type parameter '{input_length}'.")
                return

        else:
            log.error(f"Unknown operation type '{operation}' in the definition of column '{self.id}'.")
            return

        #
        # Append the newly generated column(s) to this table
        #
        self._append_output_columns(out)

        log.info(f"<--- Finish evaluating column '{self.id}'")

    def _evaluate_calc_value(self, func, data, data_type, model):
        """Calculate column. Apply function to each row of the table."""

        #
        # Single input: Apply to a series. UDF will get single value
        #
        if isinstance(data, pd.Series) or ((isinstance(data, pd.DataFrame) and len(data.columns) == 1)):
            if isinstance(data, pd.DataFrame):
                ser = data[data.columns[0]]
            else:
                ser = data

            if model is None:
                out = pd.Series.apply(ser, func)  # Do not pass model to the function
            elif isinstance(model, (list, tuple)):
                out = pd.Series.apply(ser, func, *model)  # Model as positional arguments
            elif isinstance(model, dict):
                # Pass model by flattening dict (alternative: arbitrary Python object as positional or key argument). UDF has to declare the expected arguments
                out = pd.Series.apply(ser, func, **model)  # Model as keyword arguments
            else:
                out = pd.Series.apply(ser, func, args=(model,))  # Model as an arbitrary object

        #
        # Multiple inputs: Apply to a frame. UDF will get a row of values
        #
        elif isinstance(data, pd.DataFrame):
            # Notes:
            # - UDF expects one row as a data input (raw=True - ndarry, raw=False - Series)
            # - model (arguments) cannot be None, so we need to guarantee that we do not pass None

            if data_type == 'ndarray':
                out = pd.DataFrame.apply(data, func, axis=1, raw=True, **model)
            else:
                if model is None:
                    out = pd.DataFrame.apply(data, func, axis=1, raw=False)  # Do not pass model to the function
                elif isinstance(model, (list, tuple)):
                    out = pd.DataFrame.apply(data, func, axis=1, raw=False, *model)  # Model as positional arguments
                elif isinstance(model, dict):
                    out = pd.DataFrame.apply(data, func, axis=1, raw=False, **model)  # Model as keyword arguments
                else:
                    out = pd.DataFrame.apply(data, func, axis=1, raw=False, args=(model,))  # Model as an arbitrary object

        else:
            log.error(f"Unknown input data type '{type(data_type).__name__}'")

        return out

    def _evaluate_calc_column(self, func, data, data_type, model):
        """Calculate column. Apply function to all inputs and return calculated column(s)."""

        #
        # Cast to the necessary argument type expected by the function
        #
        if data_type == 'ndarray':
            data_arg = data.values
            data_arg.reshape(-1, 1)
        elif (isinstance(data, pd.DataFrame) and len(data.columns) == 1):
            # data_arg = data
            data_arg = data[data.columns[0]]
        else:
            data_arg = data

        if isinstance(model, dict):
            out = func(data_arg, **model)  # Model as keyword arguments
        elif isinstance(model, (list, tuple)):
            out = func(data_arg, *model)  # Model as positional arguments
        else:
            out = func(data_arg, model)  # Model as an arbitrary object

        return out

    def _evaluate_link(self):
        """Link column. Output column will store ids (indexes) of the target table rows."""
        definition = self.definition

        #
        # Read and validate parameters
        #

        main_table_name = definition.get('table')
        main_table = self.schema.get_table(main_table_name)

        outputs = definition.get('outputs')
        column_name = outputs[0]
        output_column = self.schema.get_column(main_table_name, column_name)

        main_keys = self.get_columns()
        if not all_columns_exist(main_keys, main_table.data):
            log.error("Not all key columns available in the link column definition.".format())
            return

        linked_table_name = output_column.definition.get('type', '')
        linked_table = self.schema.get_table(linked_table_name)
        if not linked_table:
            log.error("Linked table '{0}' cannot be found in the link column definition..".format(linked_table))
            return

        linked_columns = definition.get('linked_columns', [])
        if len(linked_columns) == 0:
            linked_columns = linked_table.definition.get("attributes", [])  # By default (e.g., for projection), we link to target table attributes
        if not all_columns_exist(linked_columns, linked_table.data):
            log.error("Not all linked key columns available in the link column definition.".format())
            return

        #
        # 1. In the target (linked) table, convert its index into a normal column (because we can only merge normal columns and index)
        #
        index_column_name = '__row_id__' # It could be 'id', 'index' or whatever other convention
        linked_table.data[index_column_name] = linked_table.data.index
        # df.reset_index(inplace=True).set_index('index', drop=False, inplace=True)  ä Alternative 1: reset will convert index to column, and then again create index
        # df = df.rename_axis('index1').reset_index() # Alternative 2: New index1 column will be created

        #
        # 2. Create left join on the specified keys
        #

        linked_prefix = column_name+'::'  # It will be prepended to each linked (secondary) column name

        out_df = pd.merge(
            main_table.data,  # This table
            linked_table.data.rename(columns=lambda x: linked_prefix + x, inplace=False),  # Target table to link to. We rename columns (not in place - the original frame preserves column names)
            how='left',  # This (main) table is not changed - we attach target records
            left_on=main_keys,  # List of main table key columns
            right_on= [linked_prefix + x for x in linked_columns],  # List of target table key columns. Note that we renamed them above so we use modified names.
            left_index=False,
            right_index=False,
            #suffixes=('', linked_suffix),  # We do not use suffixes because they cannot be enforced (they are used only in the case of equal column names)
            sort=False  # Sorting decreases performance
        )

        # We do not need this column anymore - it was merged into the result as a link column
        linked_table.data.drop(columns=[index_column_name], inplace=True)

        #
        # 3. Rename according to our convention and store the result
        #

        # Rename our link column by using only specified column name
        out_df.rename({column_name+'::'+index_column_name: column_name}, axis='columns', inplace=True)

        out = out_df[column_name]

        return out

    def _evaluate_merge(self):
        """Merge column. Materialize a complex column path which is sequence of link columns ending with some target column."""
        definition = self.definition

        #
        # Read and validate parameters
        #
        output_table_name = definition.get('table')
        output_table = self.schema.get_table(output_table_name)
        output_table_data = output_table.get_data()

        outputs = definition.get('outputs')
        output_column_name = outputs[0]
        output_column = self.schema.get_column(output_table_name, output_column_name)

        column_segment_separator = '::'

        columns = self.get_columns()
        link_column_path = ''  # Column path composed of several separated column segment names
        df = output_table_data
        main_table = output_table
        for i, link_column_name in enumerate(columns):
            #
            # Build link column path by appending a new column segment name
            # Note that this path is a name of the column in the data frame and it does not have a definition
            #
            if i == 0:
                link_column_path = link_column_name
            else:
                link_column_path += column_segment_separator + link_column_name

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
            linked_table_name = link_column.definition.get('type')
            linked_table = self.schema.get_table(linked_table_name)
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
            main_table = linked_table

        return out

    def _merge_two_columns(self, source_df, link_column_name, target_df, linked_column_name):
        """Given source and target tables with link and linked columns, return a new column with the source table index and target column values."""

        linked_prefix = link_column_name + '::'  # It will prepended to each linked (secondary) column name

        out_df = pd.merge(
            source_df[[link_column_name]],
            target_df[[linked_column_name]].rename(columns=lambda x: linked_prefix + x, inplace=False),  # We rename target columns to avoid conflicts (not in place - the original frame preserves column names)
            how='left',  # This (source) table is not changed - we want to attach target records
            left_on=link_column_name,  # Its values are row ids stored in the linked table index
            right_on=None,
            left_index=False,
            right_index=True,  # Use index because link column stores index values
            #suffixes=('', linked_suffix),  # We do not use suffixes because they cannot be enforced (they are used only in the case of equal column names)
            sort=False  # Sorting decreases performance
        )
        # Here we get linked column names like 'Prefix::OriginalName'

        return out_df

    def _evaluate_roll_column(self, func, data, data_type, model):
        """Roll column. Apply aggregate function to each window defined on this same table for every record."""
        definition = self.definition

        #
        # Determine input columns
        #
        columns = self.get_columns()
        columns = get_columns(columns, data)
        if columns is None:
            log.warning("Error reading input column list. Skip column definition.")
            return

        # Validation: check if all explicitly specified columns available
        if not all_columns_exist(columns, data):
            log.warning("Not all input columns available. Skip column definition.".format())
            return

        #
        # Determine window size. The window parameter can be string, number or object (many arguments for rolling object)
        #
        window = definition.get('window')
        window_size = int(window)
        rolling_args = {'window': window_size}
        # TODO: try/catch with log message if cannot get window size

        #
        # Single input. Moving aggregation of one input column. Function will get a sub-series as a data argument
        #
        if len(columns) == 1:

            in_column = columns[0]

            # Create a rolling object with windowing (row-based windowing independent of the number of columns)
            by_window = pd.DataFrame.rolling(data, **rolling_args)  # as_index=False - aggregations will produce flat DataFrame instead of Series with index

            # Apply function to all windows
            if data_type == 'ndarray':
                out = by_window[in_column].apply(func, raw=True, **model)
            else:
                out = by_window[in_column].apply(func, raw=False, **model)

        #
        # Multiple inputs. Function will get a sub-dataframe as a data argument
        #
        else:

            #
            # Workaround: create a new temporary data frame with all row ids, create a rolling object by using it, apply UDF to it, the UDF will get a window/group of row ids which can be then used to access this window rows from the main data frame:
            # Link: https://stackoverflow.com/questions/45928761/rolling-pca-on-pandas-dataframe
            #

            df_idx = pd.DataFrame(np.arange(data.shape[0]))  # Temporary data frame with all row ids like 0,1,2,...
            idx_window = df_idx.rolling(**rolling_args)  # Create rolling object from ids-frame

            # Auxiliary function creates a subframe with data and passes it to the user function
            def window_fn(ids, user_f):
                return user_f(data.iloc[ids])

            out = idx_window.apply(lambda x: window_fn(x, func), raw=False)  # Both Series and ndarray work (for iloc)

        return out

    def _evaluate_group_column(self, func, model):
        """Group column. Apply aggregate function to each group of records of the fact table."""
        definition = self.definition

        #
        # Get parameters
        #

        tables = definition.get('tables')
        source_table_name = tables[0]
        source_table = self.schema.get_table(source_table_name)
        if source_table is None:
            log.error("Cannot find the fact table '{0}'.".format(source_table_name))
            return

        link_column_name = definition.get('link')
        link_column = source_table.get_column(link_column_name)
        if link_column is None:
            log.error("Cannot find the link column '{0}'.".format(link_column_name))
            return

        #
        # Build input fact frame to pass to the function
        #
        data = source_table.data

        columns = self.get_columns()
        columns = get_columns(columns, data)
        if columns is None:
            log.warning("Error reading input column list. Skip column definition.")
            return

        # Validation: check if all explicitly specified columns available
        if not all_columns_exist(columns, data):
            log.warning("Not all input columns available. Skip column definition.".format())
            return

        data = data[columns]  # Select only the specified input columns

        data_type = definition.get('data_type')

        #
        # Group by the values (ids) of the link column. All facts with the same id in the link column belong to one group
        #
        gb = self._get_groupby()

        if len(columns) == 0:  # Special case: no input columns (or function is size()
            out = gb.size()
        if len(columns) == 1:  # Single input: udf will get a sub-series with fact values
            out = gb[columns[0]].agg(func, **model)  # Apply function to all groups
        else:  # Multiple inputs. Function will get a sub-dataframe as a data argument
            # TODO:
            pass

        return out

    def _get_groupby(self):
        """Each link column stores a pandas groupby object. Return or build such a groupby object for the (already evaluated) link column specified in this definition."""

        definition = self.definition

        tables = definition.get('tables')
        source_table_name = tables[0]
        source_table = self.schema.get_table(source_table_name)

        link_column_name = definition.get('link')
        link_column = self.schema.get_column(source_table_name, link_column_name)

        if link_column.groupby is not None:
            return link_column.groupby

        # Use link column (with target row ids) to build a groupby object (it will build a group for each target row id)
        try:
            gb = source_table.data.groupby(link_column_name, as_index=True)
            # Alternatively, we could use target keys or main keys
        except Exception as e:
            log.error("Error grouping input table using the specified column(s).".format())
            log.debug(e)
            raise e

        # TODO: We might want to remove a group for null value (if it is created by the groupby constructor)

        link_column.groupby = gb

        return gb

    def _append_output_columns(self, out):
        """Append the specified column(s) to the dataframe of their table."""
        definition = self.definition

        outputs = self.get_outputs()
        output_table_name = definition.get('table')
        output_table = self.schema.get_table(output_table_name)

        fillna_value = definition.get('fillna_value')

        #
        # Post-process the result by renaming the output columns accordingly (some convention is needed to know what output to expect)
        #
        # TODO: The result can be Series/listndarray(1d or 2d) and we need to convert it to DataFrame by using the original index.
        out = pd.DataFrame(out)  # Result can be ndarray so we convert to data frame
        for i, c in enumerate(out.columns):

            # Determine column name for this result
            if outputs and i < len(outputs):  # Explicitly specified output column name
                attached_column_name = outputs[i]
            else:  # Same name - overwrite input column
                columns = self.get_columns()
                columns = get_columns(columns, output_table.data)
                attached_column_name = columns[i]

            #
            # Attach this new column name to the output table data frame
            #
            data = output_table.get_data()
            data[attached_column_name] = out[c]  # A column is attached by matching indexes so indexes have to be consistent (the same)

            if fillna_value is not None:
                data[attached_column_name].fillna(fillna_value, inplace=True)


if __name__ == "__main__":
    pass
