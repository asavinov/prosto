import json
from collections import namedtuple

from prosto.utils import *

import logging
log = logging.getLogger('prosto.data')


Range = namedtuple('Range', 'start end')

class Data:
    """The class represents data physically stored as a pandas data frame."""
    # INFO:
    # d = self.df.iloc[0].name
    # d = self.df.index[0]
    # id = self.df.index.min()
    # id = self.df.first_valid_index()
    # max_id = self.df.index.max()
    # max_id = self.df.last_valid_index()

    data_no = 0

    def __init__(self, table):
        """
        Create a new table object using name and attributes.

        :param name: Name of the data object which should be equal to the table name it represents
        :param definition: Table attributes
        """

        # Assign id
        self.id = None
        if self.id is None:
            self.id = "___data___" + str(Data.data_no)
            Data.data_no += 1

        # Store table it belongs to
        self.table = table

        # Data frame which store the real data for this table (all its attributes and columns)
        attributes = table.definition.get("attributes", [])
        self.df = pd.DataFrame(columns=attributes, index=pd.Int64Index([]))
        self.df.name = table.id
        self.df.index.astype(int)

        # Track changes
        self.removed_range = Range(0, 0)
        self.added_range = Range(0, 0)

    def __repr__(self):
        return '['+self.id+']'

    def get_df(self):
        return self.df

    def set_df(self, df):
        self.df = df
        self.df.name = self.table.id

    def get_series(self, column_name):
        return self.df[column_name]

    def all_columns_exist(self, names):
        columns = self.df.columns
        for col in names:
            if col not in columns:
                return False
        return True

    #
    # Read column data
    #
    def get_values(self, column_name):
        """Read column values"""
        return self.df[column_name]

    def get_full_slice(self, columns):
        """Get a slice with all rows (without removed) and specified columns"""

        start_id = self.removed_range.end
        end_id = self.added_range.end

        ret = self.df.loc[start_id:end_id, columns]

        return ret

    def get_added_slice(self, columns):
        """Get a slice with added rows and specified columns"""

        start_id = self.added_range.start
        end_id = self.added_range.end

        ret = self.df.loc[start_id:end_id, columns]

        return ret

    #
    # Write column data
    #

    def set_column_values_for_range(self, update, range, default_value):
        """
        Impose columns from the specified data frame onto this data by overwriting existing cells using index for both columns and rows.
        Set new values for the specified range by overwriting existing values by those from the update frame or (if they are absent) by default value (which can be NaN).
        It is guaranteed that values in the specified range will be changed, that is, old values from the specified range will be removed.
        New values are taken either from the update frame or default value.
        If a column is absent in the target then, it will be added.
        If a row is absent in the target then, it will NOT be added.
        If a row absent in the source, then it will not be updated.
        """

        # TODO: Shorten update frame to the specified range if necessary (or at least check that it is inside the range and warn if not)

        #
        # Update columns by ensuring that new columns exist
        #
        for col in update.columns.to_list():
            if col not in self.df.columns.to_list():
                self.df[col] = default_value

        #
        # Update values
        #

        if range is None:
            range = self.id_range()  # Full range

        # Approach 1:
        # 1) Assign default value to the data in the specified full range (essentially do reset)
        self.df.loc[range.start:range.end, update.columns.to_list()] = default_value
        # 2) Impose values from the update frame on the data. Missing values will not be changed and hence will be equal to default value.
        self.df.update(update, overwrite=True)
        # INFO: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.update.html
        # - if source is NA, then it is ignored (independent of the target)
        # - if target is NA: if overwrite=True then overwrite, elif overwrite=False then leave NA

        # Approach 2:
        # 1) Extend the update index to the specified (full) range by setting added rows to the default value
        # 2) Assign this extended update frame to the data using its index (which has full necessary range)

        #
        # Theoretically, since we change the values, we need to mark this range as changed/dirty
        # Yet, tracking value updates is currently not supported
        #

        return range.end - range.start

    #
    # Add rows
    #

    def add(self):
        """Add one row and return its id. All attributes and columns will have empty values (None)."""
        empty_value = None
        first_id = self._get_next_id()

        # Approach 1. Specify new index value explicitly
        self.df.loc[first_id, :] = empty_value

        # Approach 2. Series name will be used as new index value
        #self.df = self.df.append(pd.Series(name=first_id, data=[empty_value]), sort=False)

        # Track changes
        self.extend_added(1)

        return first_id

    def add(self, count):
        """Add several rows."""
        empty_value = None
        first_id = self._get_next_id()

        # Empty data frame with new (added) row ids in the index
        new_ids = range(first_id, first_id + count)
        table = pd.DataFrame(index=new_ids)

        # Approach 1
        self.df = self.df.append(table, sort=False)

        # Approach 2
        #self.df = pd.concat([self.df, table])

        # Track changes
        self.extend_added(count)

        return first_id

    def add(self, record):
        """Add one new row with the specified attribute values passed as a dictionary or series"""
        empty_value = None
        first_id = self._get_next_id()

        # Approach 1. Specify new index value explicitly
        self.df.loc[first_id, :] = record  # We can assign both Series and dict types

        # Approach 2. Series name will be used as new index value
        #if isinstance(record, dict):
        #    record = pd.Series(record, name=first_id)
        #self.df = self.df.append(record, sort=False)

        # Track changes
        self.extend_added(1)

        return first_id

    def add(self, table):
        """Add multiple new rows with the specified attribute values passed as a structured which is a dataframe or can be used to instantiate a data frame."""
        empty_value = None
        first_id = self._get_next_id()

        # Data frame with new (added) row ids in the index and data to be appended
        count = len(table)
        new_ids = range(first_id, first_id + count)
        table = pd.DataFrame(table, index=new_ids)  # Even if it is already a data frame, we want to explicitly set its index

        # Approach 1
        self.df = self.df.append(table, sort=False)

        # Approach 2
        #self.df = pd.concat([self.df, table])

        # Track changes
        self.extend_added(count)

        return first_id

    #
    # Physically delete records and manage allocated space
    #

    def gc(self):
        """Physically delete all records which are not used, that is, their removal was already propagated."""
        if self.df.empty:
            return
        start = self.df.index[0]
        end = self.removed_range.start
        to_delete = range(start, end)
        self.df.drop(to_delete, inplace=True)
        #self.df = self.df.iloc[len(to_delete):]

    def reset(self):
        """Physically remove all records and start from new empty table with no tracking."""

        #self.df.drop(self.df.index, inplace=True)
        #self.df = pd.DataFrame(columns=self.df.columns)
        self.df = self.df.iloc[0:0]

        self.df.name = self.table.id  # name is not copied for some reason

        # Track changes
        self.added_range = Range(0, 0)
        self.removed_range = Range(0, 0)

    #
    # Track changes
    #

    def id_range(self):
        return Range(self.removed_range.end, self.added_range.end)

    def length(self):
        return self.added_range.end - self.removed_range.end  # All non-removed records

    def added_length(self):
        return self.added_range.end - self.added_range.start

    def removed_length(self):
        return self.removed_range.end - self.removed_range.start

    def extend_added(self, count=1):
        self.added_range = Range(self.added_range.start, self.added_range.end + count)

    def extend_removed(self, count=1):
        self.removed_range = Range(self.removed_range.start, self.removed_range.end + count)

    def shrink_added(self):
        added = self.added_range.end - self.added_range.start
        self.added_range = Range(self.added_range.end, self.added_range.end)
        return added

    def shrink_removed(self):
        removed = self.removed_range.end - self.removed_range.start
        self.removed_range = Range(self.removed_range.end, self.removed_range.end)
        return removed

    def clear_change_status(self):
        added = self.shrink_added()
        removed = self.shrink_removed()

    #
    # Remove rows (mark for removal)
    #

    def remove(self):
        """Mark one oldest record as removed."""

        if self.length() > 0:
            # Extend removed range by 1
            self.removed_range.end = Range(self.removed_range.start, self.removed_range.end + 1)

    def remove(self, count):
        """Mark the specified number of oldest records as removed."""

        to_remove = min(count, self.length())
        if to_remove > 0:
            # Extend removed range by count
            self.removed_range = Range(self.removed_range.start, self.removed_range.end + to_remove)

        return Range(self.removed_range.end - to_remove, self.removed_range.end)

    def remove_all(self):
        """Mark all records as removed."""

        # Track changes
        if self.length() > 0:
            # Extend removed range till the end of added range (added records are also removed)
            self.removed_range = Range(self.removed_range.start, self.added_range.end)

    #
    # Convenience methods
    #

    def _get_next_id(self):
        return self.added_range.end

    def _get_start_offset(self):
        """Physically existing records"""
        return 0

    def _get_end_offset(self):
        """Physically existing records"""
        return len(self.df)


if __name__ == "__main__":
    pass
