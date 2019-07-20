import unittest

from prosto.Schema import *

class TablePopulateTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_populate(self):
        sch = Schema("My schema")

        tbl = sch.create_populate_table(
            table_name="My table", attributes=["A", "B"],
            func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0], 'B': ['x', 'y', 'z']})", tables=[], model={"nrows": 3}, input_length='table'
        )

        tbl.evaluate()

        self.assertEqual(len(tbl.data.columns), 2)
        self.assertEqual(len(tbl.data), 3)


if __name__ == '__main__':
    unittest.main()
