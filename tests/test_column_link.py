import unittest

from prosto.Prosto import *

class ColumnLinkTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_one_key(self):
        sch = Prosto("My Prosto")

        # Facts
        f_tbl = sch.populate(
            table_name="Facts", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b']})", tables=[], model=None, input_length='table'
        )

        # Groups
        g_tbl = sch.populate(
            table_name="Groups", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c']})", tables=[], model=None, input_length='table'
        )

        # Link
        l_clm = sch.create_link_column(
            name="Link", table=f_tbl.id, type=g_tbl.id,
            columns=["A"], linked_columns=["A"]
        )

        f_tbl.evaluate()
        g_tbl.evaluate()

        l_clm.evaluate()

        f_tbl_data = f_tbl.get_data()
        self.assertEqual(len(f_tbl_data), 4)  # Same number of rows
        self.assertEqual(len(f_tbl_data.columns), 2)

        l_data = f_tbl.get_column_data("Link")
        self.assertEqual(l_data[0], 0)
        self.assertEqual(l_data[1], 0)
        self.assertEqual(l_data[2], 1)
        self.assertEqual(l_data[3], 1)

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()  # All data will be reset
        layers = topology.elem_layers

        self.assertEqual(len(layers), 2)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["Facts", "Groups"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["Link"]))

    def test_two_keys(self):
        sch = Prosto("My Prosto")

        # Facts
        f_tbl = sch.populate(
            table_name="Facts", attributes=["A", "B"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'b', 'a'], 'B': ['b', 'c', 'c', 'a']})", tables=[], model=None, input_length='table'
        )

        # Groups
        g_tbl = sch.populate(
            table_name="Groups", attributes=["A", "B"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'a'], 'B': ['b', 'c', 'c'], 'C': [1, 2, 3]})", tables=[], model=None, input_length='table'
        )

        # Link
        l_clm = sch.create_link_column(
            name="Link", table=f_tbl.id, type=g_tbl.id,
            columns=["A", "B"], linked_columns=["A", "B"]
        )

        f_tbl.evaluate()
        g_tbl.evaluate()

        l_clm.evaluate()

        f_tbl_data = f_tbl.get_data()
        self.assertEqual(len(f_tbl_data), 4)  # Same number of rows
        self.assertEqual(len(f_tbl_data.columns), 3)

        l_data = f_tbl.get_column_data("Link")
        self.assertEqual(l_data[0], 0)
        self.assertEqual(l_data[1], 1)
        self.assertEqual(l_data[2], 1)
        self.assertTrue(pd.isna(l_data[3]))

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()  # All data will be reset
        layers = topology.elem_layers

        self.assertEqual(len(layers), 2)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["Facts", "Groups"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["Link"]))

        sch.run()

        l_data = f_tbl.get_column_data("Link")
        self.assertEqual(l_data[0], 0)
        self.assertEqual(l_data[1], 1)
        self.assertEqual(l_data[2], 1)
        self.assertTrue(pd.isna(l_data[3]))


if __name__ == '__main__':
    unittest.main()
