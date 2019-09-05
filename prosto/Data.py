import json

from prosto.utils import *

import logging
log = logging.getLogger('prosto.data')


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
        self.df = pd.DataFrame()
        self.df.name = table.id

        # Added range

        # Deleted range

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
    # Add rows
    #
    # TODO: We need to use current field for last/first row ids (or dirty/added/removed ranges) and also update them after adding record(s)

    def add(self):
        """Add one row and return its id. All attributes and columns will have empty values (None)."""
        empty_value = None
        first_id = self._get_next_id()

        # Approach 1. Specify new index value explicitly
        self.df.loc[first_id, :] = empty_value

        # Approach 2. Series name will be used as new index value
        #self.df = self.df.append(pd.Series(name=first_id, data=[empty_value]))

        return first_id

    def add(self, count):
        """Add several rows."""
        empty_value = None
        first_id = self._get_next_id()

        # Empty data frame with new (added) row ids in the index
        new_ids = range(first_id, first_id + count)
        table = pd.DataFrame(index=new_ids)

        # Approach 1
        self.df = self.df.append(table)

        # Approach 2
        #self.df = pd.concat([self.df, table])

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
        #self.df = self.df.append(record)

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
        self.df = self.df.append(table)

        # Approach 2
        #self.df = pd.concat([self.df, table])

        return first_id

    #
    # Remove rows
    #

    def remove(self):
        """Remove all records"""

        self.df = self.df.iloc[0:0]
        #self.df = pd.DataFrame(columns=self.df.columns)
        #self.df.drop(self.df.index, inplace=True)

        self.df.name = self.table.id  # name is not copied for some reason

    #
    # Read columns
    #
    def get_values(self, column_name):
        """Read column values"""
        return self.df[column_name]

    #
    # Write columns
    #
    def set_values(self, column_name, values, fillna_value):
        """Write column values by overwriting existing values and creating a new column if it does not exist."""

        self.df[column_name] = values

        if fillna_value is not None:
            self.df[column_name].fillna(fillna_value, inplace=True)

    #
    # Convenience methods
    #

    def _get_next_id(self):
        if len(self.df) > 0:
            max_id = self.df.iloc[-1].name
            #max_id = self.df.index.max()
            #max_id = self.df.last_valid_index()
        else:
            max_id = -1

        return max_id + 1


if __name__ == "__main__":
    pass
