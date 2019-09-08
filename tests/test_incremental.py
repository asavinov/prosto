import unittest

from prosto.Schema import *

class IncrementalTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_calculate_value(self):
        sch = Schema("My schema")

        tbl = sch.create_table(
            table_name="My table", attributes=["A"],
        )

        clm = sch.create_calculate_column(
            name="My column", table=tbl.id,
            func="lambda x: float(x)", columns=["A"], model=None, input_length='value'
        )

        sch.run()  # Inference on empty data

        tbl.data.add({"A": 1})  # New record is added and marked as added

        # Assert new change status
        self.assertEqual(tbl.data.added_length(), 1)

        sch.run()

        # Assert clean change status and results of inference
        self.assertEqual(tbl.data.added_length(), 0)

        tbl.data.add({"A": 2})
        tbl.data.add({"A": 3})

        # Assert new change status
        self.assertEqual(tbl.data.added_length(), 2)

        sch.run()

        # Assert clean change status and results of inference
        self.assertEqual(tbl.data.added_length(), 0)

        tbl.data.remove(1)  # Remove one oldest record by marking it as removed

        # Assert new change status
        self.assertEqual(tbl.data.removed_length(), 1)

        sch.run()

        # Assert clean change status and results of inference
        self.assertEqual(tbl.data.removed_length(), 0)

        tbl.data.remove_all()  # Remove all records by marking them as removed

        # Assert new change status

        sch.run()

        # Assert clean change status and results of inference
        self.assertEqual(tbl.data.added_length(), 0)
        self.assertEqual(tbl.data.removed_length(), 0)


if __name__ == '__main__':
    unittest.main()
