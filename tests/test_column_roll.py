import unittest

from prosto.Prosto import *

class ColumnRollTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_roll_single(self):
        sch = Prosto("My Prosto")

        tbl = sch.populate(
            table_name="My table", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0]})", tables=[], model=None, input_length='table'
        )

        clm = sch.roll(
            name="Roll", table=tbl.id,
            window="2",
            func="lambda x: x.sum()", columns=["A"], model={}, input_length='column'
        )

        tbl.evaluate()
        clm.evaluate()

        clm_data = tbl.get_column_data('Roll')

        self.assertTrue(pd.isna(clm_data[0]))
        self.assertAlmostEqual(clm_data[1], 3.0)
        self.assertAlmostEqual(clm_data[2], 5.0)

    def test_roll_multiple(self):
        sch = Prosto("My Prosto")

        tbl = sch.populate(
            table_name="My table", attributes=["A", "B"],
            func="lambda **m: pd.DataFrame({'A': [1, 2, 3], 'B': [3, 2, 1]})", tables=[], model=None, input_length='table'
        )

        clm = sch.roll(
            name="Roll", table=tbl.id,
            window="2",
            func="lambda x: x['A'].sum() + x['B'].sum()", columns=["A", "B"], model={}, input_length='column'
        )

        tbl.evaluate()
        clm.evaluate()

        clm_data = tbl.get_column_data('Roll')

        self.assertTrue(pd.isna(clm_data[0]))
        self.assertAlmostEqual(clm_data[1], 8.0)
        self.assertAlmostEqual(clm_data[2], 8.0)

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()  # All data will be reset
        layers = topology.elem_layers

        self.assertEqual(len(layers), 2)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["My table"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["Roll"]))

        sch.run()

        clm_data = tbl.get_column_data('Roll')
        self.assertTrue(pd.isna(clm_data[0]))
        self.assertAlmostEqual(clm_data[1], 8.0)
        self.assertAlmostEqual(clm_data[2], 8.0)


if __name__ == '__main__':
    unittest.main()
