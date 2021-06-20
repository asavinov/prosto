from prosto.Prosto import *


def translate_column_sql(pr: Prosto, query: str, func=None, args=None):
    # Parse query and extract parameters
    op, entries, func_str, args_str, win_str = parse_column_sql(query)

    if func is None:
        func = func_str
    if args is None:
        args = args_str

    if op == 'TABLE':
        table = entries[0][0]
        attributes = entries[0][1:]
        definition = pr.populate(
            table, attributes=attributes,
            func=func, tables=None, model=args
        )
    elif op == 'CALC':
        table = entries[0][0]
        columns = entries[0][1:]
        name = entries[1][0]
        definition = pr.calculate(
            name, table,
            func=func, columns=columns, model=None if not args else args
        )
    elif op == 'ROLL':
        table = entries[0][0]
        columns = entries[0][1:]
        name = entries[1][0]
        definition = pr.roll(
            name, table,
            window=win_str, link=None,
            func=func, columns=columns, model=None if not args else args
        )
    elif op == 'LINK':
        raise NotImplementedError(f"Column-SQL operation {op} not implemented or not known.")
    else:
        raise NotImplementedError(f"Column-SQL operation {op} not implemented or not known.")

    return definition


def parse_column_sql(query: str):
    #
    # Operation
    #
    op, query = query.strip().split(maxsplit=1)
    op = op.strip()
    query = query.strip()

    #
    # FUNC, ARGS, WINDOW
    #

    func_start = query.lower().find("func")
    args_start = query.lower().find("args")
    win_start = query.lower().find("window")

    # Alternatively, use regex
    # re.search('(?i)Mandy Pande:', line)
    # re.search('mandy', 'Mandy Pande', re.IGNORECASE)
    # re.match("mandy", "MaNdY", re.IGNORECASE)

    # FUNC
    if func_start >= 0:
        if args_start >= 0:
            func_end = args_start
        elif win_start >= 0:
            func_end = win_start
        else:
            func_end = len(query)
        func_str = query[func_start + len("FUNC"):func_end].strip()
    else:
        func_str = ""

    # ARGS
    if args_start >= 0:
        if win_start >= 0:
            args_end = win_start
        else:
            args_end = len(query)
        args_str = query[args_start + len("ARGS"):args_end].strip()
    else:
        args_str = ""

    # WINDOW
    if win_start >= 0:
        win_end = len(query)
        win_str = query[win_start + len("WINDOW"):win_end].strip()
    else:
        win_str = ""

    # Remove FUNC, ARGS, WINDOW clauses
    if func_start >= 0:
        query = query[0:func_start]
    elif args_start >= 0:
        query = query[0:args_start]
    elif win_start >= 0:
        query = query[0:win_start]

    #
    # Parse arrows into a list of sub-strings
    # Example: table(col1,co2) -> col3 -> col4 -> table(col5)
    #
    arrow_parts = query.split("->")
    arrow_parts = [ap.strip() for ap in arrow_parts]

    entries = list()
    for ap in arrow_parts:
        # Each part, parse table with slice of columns. Example: table(col1, col2)

        # TODO: If product, then entry is a comma separated list like table1(col1,col2), table2(col3) or super1, super2

        # Create comma separated list
        entry = ap.split(",")
        entry = [e.strip() for e in entry]

        # Check if there are parentheses, and remove at start. Example: table ( col1
        e_first = entry[0].split("(")
        e_first = [e.strip() for e in e_first]

        del entry[0]
        entry = e_first + entry

        # Accordingly, remove training parentheses
        entry[-1] = entry[-1].rstrip(")").strip()

        entries.append(entry)

    return op, entries, func_str, args_str, win_str
