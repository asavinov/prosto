import unittest

from prosto.Schema import *

class TableFilterTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_filter_table(self):
        sch = Schema("My schema")

        tbl = sch.create_populate_table(
            table_name="Base table", attributes=["A", "B"],
            func="lambda **m: pd.DataFrame({'A': [1.0, 2.0, 3.0], 'B': ['x', 'yy', 'zzz']})", tables=[], model=None, input_length='table'
        )

        clm = sch.create_calculate_column(
            name="filter_column", table=tbl.id,
            func="lambda x, param: (x['A'] > param) & (x['B'].str.len() < 3)",  # Return a boolean Series
            columns=["A", "B"], model={"param": 1.5}, input_length='column'
        )

        tbl.evaluate()
        clm.evaluate()

        tbl = sch.create_filter_table(
            table_name="Filtered table", attributes=["super"],
            func=None, tables=["Base table"], columns=["filter_column"]
        )

        tbl.evaluate()

        self.assertEqual(len(tbl.get_data().columns), 1)  # Only one link-attribute is created
        self.assertEqual(len(tbl.get_data()), 1)
        self.assertEqual(tbl.get_data()['super'][0], 1)

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()
        layers = topology.elem_layers

        self.assertEqual(len(layers), 3)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["Base table"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["filter_column"]))
        self.assertTrue(set([x.id for x in layers[2]]) == set(["Filtered table"]))


if __name__ == '__main__':
    unittest.main()
