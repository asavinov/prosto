import unittest

from prosto.Schema import *

class ColumnMergeTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_merge(self):
        sch = Schema("My schema")

        # Facts
        f_tbl = sch.create_populate_table(
            table_name="Facts", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b']})", tables=[], model=None, input_length='table'
        )

        # Groups
        g_tbl = sch.create_populate_table(
            table_name="Groups", attributes=["A", "B"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [1.0, 2.0, 3.0]})", tables=[], model=None, input_length='table'
        )

        # Link
        l_clm = sch.create_link_column(
            name="Link", table=f_tbl.id, type=g_tbl.id,
            columns=["A"], linked_columns=["A"]
        )

        # Merge
        m_clm = sch.create_merge_column("Merge", f_tbl.id, ["Link", "B"])

        f_tbl.evaluate()
        g_tbl.evaluate()

        l_clm.evaluate()
        m_clm.evaluate()

        f_tbl_data = f_tbl.get_data()
        self.assertEqual(len(f_tbl_data), 4)  # Same number of rows
        self.assertEqual(len(f_tbl_data.columns), 3)

        m_data = f_tbl.get_column_data("Merge")
        self.assertEqual(m_data[0], 1.0)
        self.assertEqual(m_data[1], 1.0)
        self.assertEqual(m_data[2], 2.0)
        self.assertEqual(m_data[3], 2.0)

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()
        layers = topology.elem_layers

        self.assertEqual(len(layers), 3)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["Facts", "Groups"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["Link"]))
        self.assertTrue(set([x.id for x in layers[2]]) == set(["Merge"]))

        m_data = f_tbl.get_column_data("Merge")
        m_data.values.fill(0.0)

        sch.run()

        m_data = f_tbl.get_column_data("Merge")
        self.assertEqual(m_data.to_list(), [1.0, 1.0, 2.0, 2.0])

    def test_merge_path(self):
        sch = Schema("My schema")

        # Facts
        f_tbl = sch.create_populate_table(
            table_name="Facts", attributes=["A"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'a', 'b', 'b']})", tables=[], model=None, input_length='table'
        )

        # Groups
        g_tbl = sch.create_populate_table(
            table_name="Groups", attributes=["A", "B"],
            func="lambda **m: pd.DataFrame({'A': ['a', 'b', 'c'], 'B': [2.0, 3.0, 3.0]})", tables=[], model=None, input_length='table'
        )
        # Link
        l_clm = sch.create_link_column(
            name="Link", table=f_tbl.id, type=g_tbl.id,
            columns=["A"], linked_columns=["A"]
        )

        # SuperGroups
        sg_tbl = sch.create_populate_table(
            table_name="SuperGroups", attributes=["B", "C"],
            func="lambda **m: pd.DataFrame({'B': [2.0, 3.0, 4.0], 'C': ['x', 'y', 'z']})", tables=[], model=None, input_length='table'
        )
        # SuperLink
        sl_clm = sch.create_link_column(
            name="SuperLink", table=g_tbl.id, type=sg_tbl.id,
            columns=["B"], linked_columns=["B"]
        )

        # Merge
        m_clm = sch.create_merge_column("Merge", f_tbl.id, ["Link", "SuperLink", "C"])

        f_tbl.evaluate()
        g_tbl.evaluate()
        sg_tbl.evaluate()

        l_clm.evaluate()
        sl_clm.evaluate()
        m_clm.evaluate()

        f_tbl_data = f_tbl.get_data()
        self.assertEqual(len(f_tbl_data), 4)  # Same number of rows
        self.assertEqual(len(f_tbl_data.columns), 3)

        m_data = f_tbl.get_column_data("Merge")
        self.assertEqual(m_data.to_list(), ['x', 'x', 'y', 'y'])

        #
        # Test topology
        #
        topology = Topology(sch)
        topology.translate()
        layers = topology.elem_layers

        self.assertEqual(len(layers), 3)

        self.assertTrue(set([x.id for x in layers[0]]) == set(["Facts", "Groups", "SuperGroups"]))
        self.assertTrue(set([x.id for x in layers[1]]) == set(["Link", "SuperLink"]))
        self.assertTrue(set([x.id for x in layers[2]]) == set(["Merge"]))

        m_data = f_tbl.get_column_data("Merge")
        m_data.values.fill(None)

        sch.run()

        m_data = f_tbl.get_column_data("Merge")
        self.assertEqual(m_data.to_list(), ['x', 'x', 'y', 'y'])


if __name__ == '__main__':
    unittest.main()
