import json
import copy

from prosto.utils import *

from prosto.Schema import *
from prosto.Table import *
from prosto.Column import *
from prosto.TableOperation import *
from prosto.ColumnOperation import *

import logging
log = logging.getLogger('prosto.topology')


class Topology:
    """Topology is a graph of operations built taking into account their dependencies."""

    def __init__(self, schema):

        self.schema = schema  # If schema can be mofied then make a copy: copy.deepcopy(schema)

        self.layers = []  # Graph of operations
        self.elem_layers = []  # Graph of elements

    def translate(self):
        """Build a graph of operations by analyzing table and column dependencies."""

        #
        # Collect all operations into one list
        #
        all_operations = [x for x in self.schema.operations]

        #
        # Build graph of operations by analyzing dependencies
        #

        # Empty collection of already processed elements (they can be simultaneously removed from all)
        done = []

        # Topology to be built is a list of layers in the order of execution of their operations.
        # First layer does not have dependencies. Second layer depends on the operations in the first layer and so on.
        layers = []
        while True:
            layer = []  # List of operations which depend on only operations in some previous layers

            # We will build this new layer from the available (not added to previous layers) operations
            for op in all_operations:

                if op in done:
                    continue

                #
                # Find dependencies of this operation
                #
                if isinstance(op, (TableOperation, ColumnOperation)):
                    dep_elems = op.get_dependencies()  # Get all element definitions this element depends upon
                else:
                    log.error(f"Operation '{op.id}' with unknown class found while building topology.")
                    dep_elems = []

                #
                # Find operations which generate these elements
                #
                dep_ops = []
                for dep in dep_elems:
                    if isinstance(dep, Table):
                        ops = self.schema.get_table_operations(dep.id)
                    elif isinstance(dep, Column):
                        ops = self.schema.get_column_operations(dep.table.id, dep.id)
                    else:
                        log.error(f"Element '{dep.id}' with unknown class found while building topology (only Table and Column are possible).")
                        ops = []
                    dep_ops.extend(ops)

                #
                # If all dependencies have been processed (belong to previous layers) then add this element to the current layer
                #
                if set(dep_ops) <= set(done):
                    layer.append(op)

            if len(layer) == 0:
                break

            layers.append(layer)
            done.extend(layer)

        # Layers of operations
        self.layers = layers

        # Layers of tables/columns
        elem_layers = []
        for layer in layers:
            elem_layer = []
            for op in layer:
                # Find element (column or table) this operation produces
                outputs = op.get_outputs()
                if isinstance(op, TableOperation):  # Find table
                    tables = self.schema.get_tables(outputs)
                    elem_layer.extend(tables)
                elif isinstance(op, ColumnOperation):  # Find column
                    table_name = op.definition.get("table")
                    columns = self.schema.get_columns(table_name, outputs)
                    elem_layer.extend(columns)
            elem_layers.append(elem_layer)

        self.elem_layers = elem_layers


if __name__ == '__main__':
    pass
