import unittest

from prosto.Prosto import *

class ColumnCalculateTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_calculate_value(self):
        sch = Prosto("My Prosto")

        tbl = sch.populate(
            table_name="My table", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': [1, 2, 3]})", tables=[]
        )

        clm = sch.calculate(
            name="My column", table=tbl.id,
            func="lambda x: float(x)", columns=["A"], model=None, input_length='value'
        )

        tbl.evaluate()
        clm.evaluate()

        clm_data = tbl.get_column_data('My column')
        v0 = clm_data[0]
        v1 = clm_data[1]
        v2 = clm_data[2]

        self.assertAlmostEqual(v0, 1.0)
        self.assertAlmostEqual(v1, 2.0)
        self.assertAlmostEqual(v2, 3.0)

        self.assertIsInstance(v0, float)
        self.assertIsInstance(v1, float)
        self.assertIsInstance(v2, float)

    def test_all(self):
        sch = Prosto("My Prosto")

        tbl = sch.populate(
            table_name="My table", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': [1, 2, 3]})", tables=[]
        )

        clm = sch.calculate(
            name="My column", table=tbl.id,
            func="lambda x, **model: x.shift(**model)", columns=["A"], model={"periods": -1}, input_length='column'
        )

        tbl.evaluate()
        clm.evaluate()

        clm_data = tbl.get_column_data('My column')
        self.assertAlmostEqual(clm_data[0], 2.0)
        self.assertAlmostEqual(clm_data[1], 3.0)
        self.assertTrue(pd.isna(clm_data[2]))

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()  # All data will be reset
        layers = topology.elem_layers

        self.assertEqual(len(layers), 2)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["My table"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["My column"]))

        sch.run()

        clm_data = tbl.get_column_data('My column')
        self.assertAlmostEqual(clm_data[0], 2.0)
        self.assertAlmostEqual(clm_data[1], 3.0)
        self.assertTrue(pd.isna(clm_data[2]))


if __name__ == '__main__':
    unittest.main()
