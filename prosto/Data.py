import json
from collections import namedtuple

from prosto.utils import *

import logging
log = logging.getLogger('prosto.data')


Range = namedtuple('Range', 'start end')

class Data:
    """The class represents data physically stored as a pandas data frame."""

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
        columns = table.definition.get("attributes", [])
        self.df = pd.DataFrame(columns=columns)
        self.df.name = table.id

        # Track changes
        self.added_range = Range(0, 0)
        self.removed_range = Range(0, 0)

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

    #
    # Write column data
    #
    def set_values(self, column_name, values, fillna_value):
        """Write column values by overwriting existing values and creating a new column if it does not exist."""

        self.df[column_name] = values

        if fillna_value is not None:
            self.df[column_name].fillna(fillna_value, inplace=True)

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

        to_delete = range(self._get_first_physical_id(), self.removed_range.start)
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

    # TODO: Here we assume that new records are added by extending df physically.
    #   In the general case, we could first extend df physically with empty records by allocating the space (more than we now need)
    #   and then assign df cells to the values. Then we need to use an operation of assignment rather than append/concat.
    def _get_next_id(self):
        if len(self.df) > 0:
            max_id = self.df.iloc[-1].name
            #max_id = self.df.index.max()
            #max_id = self.df.last_valid_index()
        else:
            max_id = -1

        return max_id + 1

    def _get_first_physical_id(self):
        if len(self.df) > 0:
            id = self.df.iloc[0].name
            #id = self.df.index.min()
            #id = self.df.first_valid_index()
        else:
            # TODO: It is a problem. We need to introduce a field with: next id to be allocated, last used id, etc.
            #   Then we need to use whenever possible, especially, if the dataframe is empty
            #   Alternative solution: never empty df physically (empty df means 0 row id)
            id = -1

        return id + 1

    def _get_last_physical_id(self):
        if len(self.df) > 0:
            max_id = self.df.iloc[-1].name
            #max_id = self.df.index.max()
            #max_id = self.df.last_valid_index()
        else:
            max_id = -1

        return max_id + 1


if __name__ == "__main__":
    pass
