import unittest

from prosto.Prosto import *

class TableProjectTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_one_key(self):
        sch = Prosto("My Prosto")

        # Facts
        f_tbl = sch.create_populate_table(
            table_name="Facts", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b']})", tables=[], model=None, input_length='table'
        )

        # Groups
        g_tbl = sch.create_project_table(
            table_name="Groups", attributes=["X"],
            link="Link",
            tables=["Facts"]
        )

        # Link
        l_clm = sch.create_link_column(
            name="Link", table=f_tbl.id, type=g_tbl.id,
            columns=["A"], linked_columns=["X"]
        )

        f_tbl.evaluate()
        g_tbl.evaluate()

        l_clm.evaluate()

        g_tbl_data = g_tbl.get_data()
        self.assertEqual(len(g_tbl_data), 2)
        self.assertEqual(len(g_tbl_data.columns), 1)

        l_data = f_tbl.get_column_data("Link")
        self.assertEqual(l_data[0], 0)
        self.assertEqual(l_data[1], 0)
        self.assertEqual(l_data[2], 1)
        self.assertEqual(l_data[3], 1)

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()
        layers = topology.elem_layers

        self.assertEqual(len(layers), 3)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["Facts"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["Groups"]))
        self.assertTrue(set([x.id for x in layers[2]]) == set(["Link"]))

    def test_two_keys(self):
        sch = Prosto("My Prosto")

        # Facts
        f_tbl = sch.create_populate_table(
            table_name="Facts", attributes=["A", "B"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'b', 'a'], 'B': ['b', 'c', 'c', 'a']})", tables=[], model=None, input_length='table'
        )

        # Groups
        g_tbl = sch.create_project_table(
            table_name="Groups", attributes=["X", "Y"],
            link="Link",
            tables=["Facts"]
        )

        # Link
        l_clm = sch.create_link_column(
            name="Link", table=f_tbl.id, type=g_tbl.id,
            columns=["A", "B"], linked_columns=["X", "Y"]
        )

        f_tbl.evaluate()
        g_tbl.evaluate()

        l_clm.evaluate()

        g_tbl_data = g_tbl.get_data()
        self.assertEqual(len(g_tbl_data), 3)
        self.assertEqual(len(g_tbl_data.columns), 2)

        l_data = f_tbl.get_column_data("Link")
        self.assertEqual(l_data[0], 0)
        self.assertEqual(l_data[1], 1)
        self.assertEqual(l_data[2], 1)
        self.assertEqual(l_data[3], 2)

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()
        layers = topology.elem_layers

        self.assertEqual(len(layers), 3)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["Facts"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["Groups"]))
        self.assertTrue(set([x.id for x in layers[2]]) == set(["Link"]))

        g_tbl_data = g_tbl.get_data()
        g_tbl_data.drop(g_tbl_data.index, inplace=True)  # Empty

        sch.run()

        g_tbl_data = g_tbl.get_data()
        self.assertEqual(len(g_tbl_data), 3)
        self.assertEqual(len(g_tbl_data.columns), 2)


if __name__ == '__main__':
    unittest.main()
