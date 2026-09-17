"""Plans referentially complete subsets, for `understudy subset`.

A subset starts from one root table and a filter -- "customers created this
year" -- and becomes one source query per table, so that every foreign key in
the copied rows points at a row that was copied too. A copy that isn't
referentially complete fails to load into a target with its constraints
enabled, or loads and breaks the application that reads it.

Two directions are followed:

- Down, optionally: rows of tables that reference the selected rows. A
  customer's orders, and those orders' line items.
- Up, always: the rows that anything selected references. The products those
  line items point at, and whatever the products point at in turn.

Each table's query is plain SQL: EXISTS subqueries over named common table
expressions (WITH), so it runs on all six dialects -- MySQL from 8.0, MariaDB
from 10.2 -- and needs nothing materialized between tables. Each selection is
defined once, so a query grows linearly with the size of the graph.

Cycles -- including a table that references itself, like employees.managerId --
can't be closed without recursive SQL, which the dialects don't share. They're
reported, and the caller breaks each one by ignoring a foreign key.
"""
from __future__ import annotations

from typing import Callable, Dict, Iterable, List, NamedTuple, Optional, Sequence, Set, Tuple

from .configuration import findCycle
from .databaseDialects import ForeignKey


class SubsetError(Exception):
    """The schema can't be subset as asked: a cycle, or a chain too deep."""


# The longest chain of selections, each built on the next, that a query may
# carry. A chain of N tables followed down and back up needs 2N - 1: 31 for 16
# tables, which MySQL accepts, and 33 for 17, which it refuses ("Too high
# level of nesting"). SQL Server's planning time grows steeply past this too,
# and gives out by 24 tables.
MAX_SELECTION_DEPTH = 32


class SubsetPlan(NamedTuple):
    """Tables in load order -- every table after the tables it references."""

    tables: List[str]
    queries: Dict[str, str]
    parents: Dict[str, List[str]]
    ignored: List[ForeignKey]


def _ignored(foreignKey: ForeignKey, ignore: Set[Tuple[str, str]]) -> bool:

    return any((foreignKey.table.upper(), column.upper()) in ignore for column in foreignKey.columns)


def parseIgnore(entries: Iterable[str]) -> Set[Tuple[str, str]]:
    """`table.column` entries, as upper-cased pairs."""

    parsed = set()

    for entry in entries:
        table, separator, column = entry.rpartition('.')
        if not separator or not table or not column:
            raise SubsetError('--ignore-foreign-key takes table.column, got {!r}'.format(entry))
        parsed.add((table.upper(), column.upper()))

    return parsed


class _Builder:
    """Builds each table's selection once, as a named common table expression.

    A table's rows are chosen by EXISTS over the selections of the tables it
    is connected to. Writing those selections inline would copy each one into
    every query that needs it -- and into every selection built on it -- so a
    query grows exponentially with the depth of the schema, until a database
    refuses it or, as PostgreSQL did, runs out of memory planning it. Named
    once in a WITH clause and referred to by name, every query nests only
    three levels deep and grows linearly.
    """

    def __init__(self, names: Dict[str, str], materialize: bool, quote: Callable[[str], str]) -> None:
        self.names = names
        self.materialize = materialize
        self.quote = quote
        self.counter = 0
        self.bodies: Dict[str, str] = {}
        self.dependencies: Dict[str, List[str]] = {}


    def alias(self, prefix: str) -> str:

        self.counter += 1

        return '{}{}'.format(prefix, self.counter)


    def exists(self, selection: str, pairs: Sequence[Tuple[str, str]], outerAlias: str, dependencies: List[str]) -> str:
        """EXISTS over the named `selection`, matching its columns to the outer row's.

        EXISTS rather than `(a, b) IN (...)`, because SQL Server has no
        row-value IN, and one form for single and composite keys is simpler.
        """

        inner = self.alias('s')
        conditions = ' AND '.join('{}.{} = {}.{}'.format(inner, self.quote(innerColumn), outerAlias, self.quote(outerColumn))
                                  for innerColumn, outerColumn in pairs)
        dependencies.append(selection)

        return 'EXISTS (SELECT 1 FROM {} {} WHERE {})'.format(selection, inner, conditions)


    def define(self, selection: str, table: str, conditions: Sequence[str], alias: str, dependencies: List[str]) -> None:

        self.bodies[selection] = 'SELECT * FROM {} {} WHERE {}'.format(
            self.names[table], alias, ' OR '.join('({})'.format(condition) for condition in conditions))
        self.dependencies[selection] = dependencies


    def query(self, selection: str) -> str:
        """`WITH ... SELECT * FROM selection`, defining just what it needs, each
        before its first use -- the order selections were defined in.
        """

        needed: Set[str] = set()
        pending = [selection]
        while pending:
            name = pending.pop()
            if name not in needed:
                needed.add(name)
                pending.extend(self.dependencies[name])

        keyword = 'AS MATERIALIZED' if self.materialize else 'AS'
        definitions = ',\n'.join('{} {} ({})'.format(name, keyword, self.bodies[name]) for name in self.bodies if name in needed)

        return 'WITH {}\nSELECT * FROM {}'.format(definitions, selection)


    def depth(self) -> int:
        """The most selections on any chain of references."""

        depths: Dict[str, int] = {}

        def visit(name: str) -> int:
            if name not in depths:
                depths[name] = 1 + max((visit(dependency) for dependency in self.dependencies[name]), default=0)
            return depths[name]

        return max((visit(name) for name in self.bodies), default=0)


def _traverse(parentEdges: Dict[str, List[ForeignKey]], childEdges: Dict[str, List[ForeignKey]], roots: Iterable[str],
              followChildren: bool) -> Tuple[Set[str], Set[str]]:
    """(down, included) as upper-cased names: the roots plus what references
    them, then that plus everything referenced from it.
    """

    down: Set[str] = set(roots)
    frontier = list(down)
    while followChildren and frontier:
        table = frontier.pop()
        for foreignKey in childEdges.get(table, []):
            child = foreignKey.table.upper()
            if child not in down:
                down.add(child)
                frontier.append(child)

    included: Set[str] = set(down)
    frontier = list(down)
    while frontier:
        table = frontier.pop()
        for foreignKey in parentEdges.get(table, []):
            parent = foreignKey.referencedTable.upper()
            if parent not in included:
                included.add(parent)
                frontier.append(parent)

    return down, included


def _edges(foreignKeys: Iterable[ForeignKey]) -> Tuple[Dict[str, List[ForeignKey]], Dict[str, List[ForeignKey]]]:

    parentEdges: Dict[str, List[ForeignKey]] = {}
    childEdges: Dict[str, List[ForeignKey]] = {}
    for foreignKey in foreignKeys:
        parentEdges.setdefault(foreignKey.table.upper(), []).append(foreignKey)
        childEdges.setdefault(foreignKey.referencedTable.upper(), []).append(foreignKey)

    return parentEdges, childEdges


def relatedTables(foreignKeys: Sequence[ForeignKey], roots: Iterable[str], followChildren: bool = True) -> List[str]:
    """Every table a subset rooted at `roots` would copy -- what `schema
    --related` creates. Names as the catalog reports them.
    """

    roots = list(roots)
    names = {root.upper(): root for root in roots}
    for foreignKey in foreignKeys:
        names[foreignKey.table.upper()] = foreignKey.table
        names[foreignKey.referencedTable.upper()] = foreignKey.referencedTable

    _, included = _traverse(*_edges(foreignKeys), [root.upper() for root in roots], followChildren)

    return sorted(names[table] for table in included)


def planSubset(foreignKeys: Sequence[ForeignKey], root: str, where: str, followChildren: bool = True,
               ignore: Iterable[str] = (), materialize: bool = False, quote: Optional[Callable[[str], str]] = None) -> SubsetPlan:
    """The per-table queries for a subset rooted at `root`, filtered by `where`.

    `where` is SQL in the root table's own terms, and is embedded verbatim --
    it comes from the person running the command, like a sourceQuery does.
    Table names match case-insensitively, and the generated SQL uses each
    table's name as the database reports it.

    `materialize` writes each selection as `AS MATERIALIZED`, so the database
    computes it once instead of copying it into every query that uses it --
    without it, PostgreSQL took minutes to plan a 12-table chain. Only
    PostgreSQL and SQLite 3.35+ accept the keyword; see
    DatabaseDialect.supportsMaterializedSelections.

    A subset whose selections would nest deeper than MAX_SELECTION_DEPTH raises
    SubsetError before any query is written.

    `quote` quotes a column name for the source database, so a reserved word
    works as a foreign-key column: pass
    `lambda name: quoteIdentifier(database.type, name)`. The names are the
    catalog's, so quoting keeps them meaning the same columns. Without it,
    names are written as they are.
    """

    ignoreSet = parseIgnore(ignore)
    names: Dict[str, str] = {root.upper(): root}
    for foreignKey in foreignKeys:
        names[foreignKey.table.upper()] = foreignKey.table
        names[foreignKey.referencedTable.upper()] = foreignKey.referencedTable

    active = [foreignKey for foreignKey in foreignKeys if not _ignored(foreignKey, ignoreSet)]
    ignored = [foreignKey for foreignKey in foreignKeys if _ignored(foreignKey, ignoreSet)]

    parentEdges, childEdges = _edges(active)

    rootKey = root.upper()
    down, included = _traverse(parentEdges, childEdges, [rootKey], followChildren)

    cycle = findCycle({table: [edge.referencedTable.upper() for edge in parentEdges.get(table, [])] for table in included})
    if cycle:
        described = ' -> '.join(names[table] for table in cycle)
        raise SubsetError('foreign keys form a cycle ({}), which plain SQL cannot close. Break it with '
                          '--ignore-foreign-key table.column on one of those references, and make sure the '
                          'ignored column is nullable or masked'.format(described))

    order = _topologicalOrder(included, parentEdges)
    builder = _Builder(names, materialize, quote or (lambda name: name))
    downSelection = {table: 'subset_down_{}'.format(position) for position, table in enumerate(order, start=1)}
    keptSelection = {table: 'subset_kept_{}'.format(position) for position, table in enumerate(order, start=1)}

    def downConditions(table: str, alias: str, dependencies: List[str]) -> List[str]:
        """Why a table's rows are in the subset on the way down: the root's
        filter, or a reference to a row selected on the way down.
        """

        if table == rootKey:
            return [where]

        return [
            builder.exists(downSelection[foreignKey.referencedTable.upper()], list(zip(foreignKey.referencedColumns, foreignKey.columns)), alias,
                           dependencies)
            for foreignKey in parentEdges.get(table, []) if foreignKey.referencedTable.upper() in down
            ]

    # Parents first on the way down: a row is selected for referencing a
    # selected parent row.
    for table in order:
        if table in down:
            alias = builder.alias('t')
            dependencies: List[str] = []
            builder.define(downSelection[table], table, downConditions(table, alias, dependencies), alias, dependencies)

    # Children before parents for what is kept: a parent's rows are whatever
    # its kept children reference, so each child's selection must exist first.
    for table in reversed(order):
        alias = builder.alias('t')
        dependencies = []
        conditions = downConditions(table, alias, dependencies) if table in down else []
        for foreignKey in childEdges.get(table, []):
            child = foreignKey.table.upper()
            if child in included:
                conditions.append(builder.exists(keptSelection[child], list(zip(foreignKey.columns, foreignKey.referencedColumns)), alias,
                                                 dependencies))
        builder.define(keptSelection[table], table, conditions, alias, dependencies)

    depth = builder.depth()
    if depth > MAX_SELECTION_DEPTH:
        raise SubsetError('this subset chains {} selections, each built on the next; databases refuse or struggle past {} '
                          '(a chain of about {} tables). Root it lower in the schema, follow only parents with --no-children, '
                          'or split it into subsets rooted at different tables'.format(
                              depth, MAX_SELECTION_DEPTH, MAX_SELECTION_DEPTH // 2))

    parents = {
        names[table]: sorted({names[foreignKey.referencedTable.upper()] for foreignKey in parentEdges.get(table, [])
                              if foreignKey.referencedTable.upper() in included})
        for table in order
        }

    return SubsetPlan(tables=[names[table] for table in order], queries={names[table]: builder.query(keptSelection[table]) for table in order},
                      parents=parents, ignored=ignored)


def _topologicalOrder(tables: Set[str], parentEdges: Dict[str, List[ForeignKey]]) -> List[str]:
    """Parents before children, and alphabetical where the graph doesn't decide."""

    remaining = set(tables)
    order: List[str] = []

    while remaining:
        ready = sorted(table for table in remaining
                       if not any(edge.referencedTable.upper() in remaining for edge in parentEdges.get(table, [])))
        order += ready
        remaining -= set(ready)

    return order
