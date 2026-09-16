"""Plans referentially complete subsets, for `lightweight-etl subset`.

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

Each table's query is plain SQL built from nested EXISTS subqueries, so it runs
on all six dialects and needs nothing materialized between tables. Queries grow
with the depth of the graph; that is the price of running as ordinary data jobs.

Cycles -- including a table that references itself, like employees.managerId --
can't be closed without recursive SQL, which the dialects don't share. They're
reported, and the caller breaks each one by ignoring a foreign key.
"""
from __future__ import annotations

from typing import Dict, Iterable, List, NamedTuple, Sequence, Set, Tuple

from .configuration import findCycle
from .databaseDialects import ForeignKey


class SubsetError(Exception):
    """The schema can't be subset as asked: a cycle, or an unknown table."""


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
    """Builds the nested queries, numbering aliases so no two scopes share one."""

    def __init__(self, names: Dict[str, str]) -> None:
        self.names = names
        self.counter = 0


    def alias(self, prefix: str) -> str:

        self.counter += 1

        return '{}{}'.format(prefix, self.counter)


    def exists(self, innerQuery: str, pairs: Sequence[Tuple[str, str]], outerAlias: str) -> str:
        """EXISTS over `innerQuery`, matching inner columns to the outer row's.

        EXISTS rather than `(a, b) IN (...)`, because SQL Server has no
        row-value IN, and one form for single and composite keys is simpler.
        """

        inner = self.alias('s')
        conditions = ' AND '.join('{}.{} = {}.{}'.format(inner, innerColumn, outerAlias, outerColumn) for innerColumn, outerColumn in pairs)

        return 'EXISTS (SELECT 1 FROM ({}) {} WHERE {})'.format(innerQuery, inner, conditions)


    def select(self, table: str, conditions: Sequence[str], alias: str) -> str:

        return 'SELECT * FROM {} {} WHERE {}'.format(self.names[table], alias, ' OR '.join('({})'.format(condition) for condition in conditions))


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
               ignore: Iterable[str] = ()) -> SubsetPlan:
    """The per-table queries for a subset rooted at `root`, filtered by `where`.

    `where` is SQL in the root table's own terms, and is embedded verbatim --
    it comes from the person running the command, like a sourceQuery does.
    Table names match case-insensitively, and the generated SQL uses each
    table's name as the database reports it.
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
    builder = _Builder(names)

    def downConditions(table: str, alias: str) -> List[str]:
        """Why a table's rows are in the subset on the way down: the root's
        filter, or a reference to a row selected on the way down.
        """

        if table == rootKey:
            return [where]

        return [
            builder.exists(downQueries[foreignKey.referencedTable.upper()], list(zip(foreignKey.referencedColumns, foreignKey.columns)), alias)
            for foreignKey in parentEdges.get(table, []) if foreignKey.referencedTable.upper() in down
            ]

    downQueries: Dict[str, str] = {}
    for table in order:
        if table in down:
            alias = builder.alias('t')
            downQueries[table] = builder.select(table, downConditions(table, alias), alias)

    # Children before parents: a parent's rows are whatever its selected
    # children reference, so each child's final query must exist first.
    finalQueries: Dict[str, str] = {}
    for table in reversed(order):
        alias = builder.alias('t')
        conditions = downConditions(table, alias) if table in down else []
        for foreignKey in childEdges.get(table, []):
            child = foreignKey.table.upper()
            if child in included:
                conditions.append(builder.exists(finalQueries[child], list(zip(foreignKey.columns, foreignKey.referencedColumns)), alias))
        finalQueries[table] = builder.select(table, conditions, alias)

    parents = {
        names[table]: sorted({names[foreignKey.referencedTable.upper()] for foreignKey in parentEdges.get(table, [])
                              if foreignKey.referencedTable.upper() in included})
        for table in order
        }

    return SubsetPlan(tables=[names[table] for table in order], queries={names[table]: finalQueries[table] for table in order},
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
