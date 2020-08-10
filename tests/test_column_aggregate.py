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
            func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0], 'N': [4.0, 3.0, 2.0, 1.0]})", tables=[]
        )

        # Groups
        df = pd.DataFrame({'A': ['a', 'b', 'c']})
        g_tbl = sch.populate(
            table_name="Groups", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c']})", tables=[]
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
            func="lambda x, bias, **model: x.sum() + bias", columns=["M"], model={"bias": 0.0}
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

        #
        # Aggregation of multiple columns
        #
        # Aggregation
        a_clm2 = sch.aggregate(
            name="Aggregate 2", table=g_tbl.id,
            tables=["Facts"], link="Link",
            func="lambda x, my_param, **model: x['M'].sum() + x['N'].sum() + my_param", columns=["M", "N"], model={"my_param": 0.0}
        )

        #a_clm2.evaluate()
        sch.translate()  # All data will be reset
        sch.run()  # A new column is NOT added to the existing data frame (not clear where it is)

        a_clm2_data = g_tbl.get_column_data('Aggregate 2')
        self.assertEqual(a_clm2_data[0], 10.0)
        self.assertEqual(a_clm2_data[1], 10.0)
        self.assertEqual(a_clm2_data[2], 0.0)

        pass

    def test_aggregate_with_path(self):
        """Aggregation with column paths as measures which have to be automatically produce merge operation."""
        sch = Prosto("My Prosto")

        # Facts
        f_tbl = sch.populate(
            table_name="Facts", attributes=["A", "M"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b'], 'M': [1.0, 2.0, 3.0, 4.0]})", tables=[]
        )

        # Groups
        df = pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [3.0, 2.0, 1.0]})
        g_tbl = sch.populate(
            table_name="Groups", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [3.0, 2.0, 1.0]})", tables=[]
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
            func="lambda x, bias, **model: x.sum() + bias", columns=["Link::B"], model={"bias": 0.0}
        )

        sch.run()

        a_clm_data = g_tbl.get_column_data('Aggregate')
        self.assertEqual(a_clm_data[0], 6.0)
        self.assertEqual(a_clm_data[1], 4.0)
        self.assertEqual(a_clm_data[2], 0.0)

        pass


if __name__ == '__main__':
    unittest.main()
