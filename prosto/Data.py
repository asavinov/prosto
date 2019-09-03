import json

from prosto.utils import *

import logging
log = logging.getLogger('prosto.data')


"""
Notes:

WRITING:

- Topology translator is responsible for initialization of all Data.
  -> We need a method for initializing empty Data having all attributes (and maybe all columns).
     If we initially define all columns, then we need a convenience method for getting all derived columns (names) for this table. 
     An initialized data frame is then never reset: it is only appended with new rows, delete rows, update columns
     We also can assume that row ids will never be reset.

- Table population methods return a new data frame, and then we assign it to self.data.
  -> We need a method for setting and appending a new data frame in the Data class
     set_df(new_data), - old data is deleted (either marked for deletion or completely forgotten) 
     append_df(new_data)
     ! Note that new row ids have to be generated, that is, evaluation methods are not supposed to know about ids when they generate new data.

- In future, we will need a method for removing some (old) id ranges and adding new id range
  These methods have to update the dirty status
  
- Column evaluation methods produce a new column with the same row id index
  -> we need a method which will update (overwrite or set) the current column with new values
     Currently, new column values are written using "_append_output_columns".
     We need to generalize it and then use only Data API by excluding all direct accesses to data frame. 

READING

- Table population methods need to read input tables as well as their dirty ranges of ids
  They need to get access to both attributes and columns by imposing the necessary slice of row ids
  The result will be a new data frame without row ids but the rows are supposed to get new ids
  Generally, second result should be row ids of the existing population which have to be deleted (all by default).
  In other words, a population procedure say which current rows to delete, which will stay, and which new rows will be added.

- Column evaluation methods need to get the necessary row id slice.
  Then they need to
  select the necessary input columns.
  We can assume that they will always return new outputs for the dirty row id range

STEP 1: Refactor existing code by using only Data API and no direct access to the data frame by retaining existing tests and functions.

- Replace data field by our new Data class object.
- Initialize this object in the Topology translator which has to initialized all resources.
- Final goal is to replace all direct access to dataframe by Data API in a consistent manner, that is, using some generic rules
  for what and how has to be read and written.

- Currently, we overwrite the data frame by what table population returns.
  -> instead of field overwrite, use reset_df or append_df.
     Assume that ALL old rows are removed and new data rows are appended.
     Importantly is that we keep track of continuously increasing row ids.

- Rewrite column evaluation methods so that they rely on Data methods for updating columns data
  Ensure that row ids are correct and the new values overwrite old values according to index (row ids).

- The end of this first stage is:
  - We use only Data and its API for reading/writing rows and columns
  - Row ids are stored in indexes and are correctly incremented and deleted when newly table data is produced by operations.
  - Column data is correctly update using Data API by using column(s) returned from operations.
  - In general, all operations work as previously and always produce completely new result by deleting/overwriting old results.
    The only difference is that now the physical data is managed/accessed via Data API
  - No dirty status is used.

"""

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
    # Table operations
    #

    # Add rows

    # Add set of rows

    # Remove rows

    # Remove range of rows

    #
    # Column operations
    #

if __name__ == "__main__":
    pass
