import unittest

from prosto.Prosto import *

class ColumnAggregateTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_aggregate(self):
        sch = Prosto("My Prosto")

        # Facts
        f_tbl = sch.populate(
            table_name="Facts", attributes=["A", "M"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0]})", tables=[], model=None, input_length='table'
        )

        # Groups
        df = pd.DataFrame({'A': ['a', 'b', 'c']})
        g_tbl = sch.populate(
            table_name="Groups", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c']})", tables=[], model=None, input_length='table'
        )

        # Link
        l_clm = sch.link(
            name="Link", table=f_tbl.id, type=g_tbl.id,
            columns=["A"], linked_columns=["A"]
        )

        # Aggregation
        a_clm = sch.aggregate(
            name="Aggregate", table=g_tbl.id,
            tables=["Facts"], link="Link",
            func="lambda x, bias,**model: x.sum() + bias", columns=["M"], model={"bias": 0.0}, input_length='column'
        )

        f_tbl.evaluate()
        g_tbl.evaluate()

        l_clm.evaluate()
        a_clm.evaluate()

        g_tbl_data = g_tbl.get_data()
        self.assertEqual(len(g_tbl_data), 3)  # Same number of rows
        self.assertEqual(len(g_tbl_data.columns), 2)  # One aggregate column was added (and one technical "id" column was added which might be removed in future)

        a_clm_data = g_tbl.get_column_data('Aggregate')
        self.assertEqual(a_clm_data[0], 3.0)
        self.assertEqual(a_clm_data[1], 7.0)
        self.assertEqual(a_clm_data[2], 0.0)

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()  # All data will be reset
        layers = topology.elem_layers

        self.assertEqual(len(layers), 3)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["Facts", "Groups"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["Link"]))
        self.assertTrue(set([x.id for x in layers[2]]) == set(["Aggregate"]))

        sch.run()

        a_clm_data = g_tbl.get_column_data('Aggregate')
        self.assertEqual(a_clm_data[0], 3.0)
        self.assertEqual(a_clm_data[1], 7.0)
        self.assertEqual(a_clm_data[2], 0.0)


if __name__ == '__main__':
    unittest.main()
