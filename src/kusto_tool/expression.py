"""Experimental Kusto expression API for generating queries."""

from copy import copy
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any


class attrdict:
    """A dict whose members are accessible with the . operator."""

    def __init__(self, **kwargs):
        self._dict = kwargs

    def __getattr__(self, key):
        return self._dict[key]


OP = attrdict(
    CONTAINS="contains",
    EQ="==",
    GE=">=",
    GT=">",
    HAS="has",
    IN="in",
    LE="<=",
    LT="<",
    NCONTAINS="!contains",
    NE="!=",
    NHAS="!has",
    NIN="!in",
    SUM="sum",
    MIN="min",
    MAX="max",
    AND="and",
    OR="or",
    NOT="not",
    ADD="+",
    SUB="-",
    MUL="*",
    DIV="/",
    COUNT="count",
    DCOUNT="dcount",
    AVG="avg",
    STRCAT="strcat",
    BAG_UNPACK="bag_unpack",
    PERCENTILE="percentile",
)

PTYPES = {
    bool: "bool",
    datetime: "datetime",
    Decimal: "decimal",
    dict: "dynamic",
    float: "real",
    int: "long",
    list: "dynamic",
    object: "dynamic",
    str: "string",
    timedelta: "timespan",
    tuple: "dynamic",
}

KTYPES = {
    "bool": bool,
    "datetime": datetime,
    "decimal": Decimal,
    "dynamic": object,
    "int": int,
    "long": int,
    "real": float,
    "string": str,
    "timespan": timedelta,
}


def quote(val):
    """Quote strings."""
    if isinstance(val, str):
        return repr(val)
    return str(val)


class Prefix:
    def __init__(self, op, *args, agg=False, dtype=Any):
        self.terms = args
        self.op = op
        self.agg = agg
        self.dtype = dtype

    def __str__(self):
        terms = ", ".join([quote(term) for term in self.terms])
        return f"{self.op}({terms})"


class Between:
    def __init__(self, lhs, left, right, negate=False):
        self.lhs = lhs
        self.left = left
        self.right = right
        self.negate = negate
        self.dtype = bool

    def __str__(self):
        neg = "!" if self.negate else ""
        return f"{self.lhs} {neg}between({self.left} .. {self.right})"


class Infix:
    # TODO: does this need __add__, __sub__?
    def __init__(self, op, lhs, rhs, dtype=Any):
        self.op = op
        self.lhs = lhs
        self.rhs = rhs
        self.dtype = dtype

    def __str__(self):
        if self.op in [OP.AND, OP.OR]:
            return f"({self.lhs}) {self.op} ({quote(self.rhs)})"
        return f"{self.lhs} {self.op} {quote(self.rhs)}"

    def __repr__(self):
        return f"{repr(self.lhs)} {self.op} {quote(self.rhs)}"

    def __and__(self, rhs):
        return Infix(OP.AND, self, rhs, dtype=bool)

    def __or__(self, rhs):
        return Infix(OP.OR, self, rhs, dtype=bool)

    def __invert__(self):
        return Prefix(OP.NOT, self, dtype=bool)


def typeof(expr):
    """Get the type of an expression."""
    if isinstance(expr, (Infix, Prefix, Column)):
        return expr.dtype
    return type(expr)


class Project:
    def __init__(self, *args, **kwargs):
        self.columns = list(args)
        self.renamed_columns = kwargs

    def _build_column_list(self):
        col_list = self.columns
        for k, v in self.renamed_columns.items():
            col_list.append(f"{k} = {v}")
        col_str = ",\n\t".join([str(col) for col in col_list])
        return col_str

    def __str__(self):
        column_list = self._build_column_list()
        return f"| project\n\t{column_list}"


class Count:
    def __repr__(self):
        return "Count()"

    def __str__(self):
        return "| count"


class Sample:
    """Sample operator."""

    def __init__(self, n):
        self.n = n

    def __repr__(self):
        return f"Sample({self.n})"

    def __str__(self):
        return f"| sample {self.n}"


class SampleDistinct:
    """Sample distinct operator."""

    def __init__(self, n, of):
        self.n = n
        self.of = of

    def __repr__(self):
        return f"Sample({self.n} of {self.of})"

    def __str__(self):
        return f"| sample {self.n} of {self.of}"


class Distinct:
    def __init__(self, *args):
        self.columns = list(args)

    def _build_column_list(self):
        col_str = ",\n".join([str(col) for col in self.columns])
        return col_str

    def __repr__(self):
        cols = ", ".join([str(col) for col in self.columns])
        return f"Distinct({cols})"

    def __str__(self):

        return f"| distinct {self._build_column_list()}"


class Where:
    def __init__(self, *args):
        self.expressions = list(args)

    def __repr__(self):
        exprs = " and ".join([repr(ex) for ex in self.expressions])
        return f"Where({exprs})"

    def __str__(self):
        exprs = " and ".join([str(ex) for ex in self.expressions])
        return f"| where {exprs}"


class Join:
    def __init__(self, right, on, kind, strategy=None):
        self.right = right
        self.on = [on] if isinstance(on, str) else on
        self.kind = kind
        if strategy in ["broadcast", "shuffle"]:
            self.strategy = strategy
        else:
            self.strategy = None

    def __str__(self):
        on_list = ", ".join([str(col) for col in self.on])
        if self.strategy:
            strategy_str = f"hint.strategy={self.strategy} "
        else:
            strategy_str = ""
        return f"| join kind={self.kind} {strategy_str}(\n\t{self.right}) on {on_list}"


class Summarize:
    def __init__(self, by=None, shuffle=False, shufflekey=None, num_partitions=None, **kwargs):
        # expressions in summarize must be aggregate functions.
        for _, v in kwargs.items():
            assert v.agg
        self.expressions = kwargs
        if by is None:
            self.by = []
        elif isinstance(by, (str, Column)):
            self.by = [by]
        else:
            self.by = by
        # shufflekey takes precedence over shuffle. If shufflekey, then shuffle.
        self.shuffle = bool(shuffle or shufflekey)
        if shufflekey:
            if isinstance(shufflekey, (str, Column)):
                self.shufflekey = [shufflekey]
            else:
                self.shufflekey = shufflekey
        else:
            self.shufflekey = []
        # num_partitions is ignored unless shuffle or shufflekey
        if self.shuffle:
            self.num_partitions = num_partitions

    def __str__(self):
        if self.expressions:
            expr_list = []
            for k, v in self.expressions.items():
                expr_list.append(f"{str(k)}={str(v)}")
            expr_list = ",\n\t".join(expr_list)
            expr_list = f"\n\t{expr_list}"
        else:
            expr_list = ""
        by_list = ", ".join([str(col) for col in self.by])
        if by_list:
            by_list = f"\n\tby {by_list}"
        shuffle_str = ""
        if self.shuffle:
            if self.shufflekey:
                key = ", ".join([str(col) for col in self.shufflekey])
                shuffle_str = f" hint.shufflekey={key}"
            else:
                shuffle_str = " hint.strategy=shuffle"
        partition_str = ""
        if self.shuffle and self.num_partitions:
            partition_str = f" hint.num_partitions={self.num_partitions}"
        clause = f"| summarize{shuffle_str}{partition_str}{expr_list}{by_list}"

        return clause


class Extend:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ",\n\t".join([f"{k}={quote(v)}" for k, v in self.kwargs.items()])
        return f"| extend\n\t{kvs}"


class Property:
    def __init__(self, column, prop):
        self.column = column
        self.prop = prop

    def __str__(self):
        return f"{str(self.column)}.{str(self.prop)}"


class ListLit:
    def __init__(self, *args):
        self.args = list(args)

    def __str__(self):
        in_list = f"({', '.join([quote(arg) for arg in self.args])})"
        return in_list


class Column:
    """A column in a tabular expression."""

    def __init__(self, name: str, dtype: str, ascending: bool = False):
        """"""
        self.name = name
        self.dtype = dtype
        self.ascending = ascending

    def __str__(self):
        return self.name

    def __invert__(self):
        return Prefix(OP.NOT, self, dtype=bool)

    def __eq__(self, rhs):
        return Infix(OP.EQ, self, rhs, dtype=bool)

    def __ne__(self, rhs):
        return Infix(OP.NE, self, rhs, dtype=bool)

    def __lt__(self, rhs):
        return Infix(OP.LT, self, rhs, dtype=bool)

    def __le__(self, rhs):
        return Infix(OP.LE, self, rhs, dtype=bool)

    def __gt__(self, rhs):
        return Infix(OP.GT, self, rhs, dtype=bool)

    def __ge__(self, rhs):
        return Infix(OP.GE, self, rhs, dtype=bool)

    def __add__(self, rhs):
        dtype = int if typeof(self) == typeof(rhs) == int else float
        return Infix(OP.ADD, self, rhs, dtype=dtype)

    def __sub__(self, rhs):
        dtype = int if typeof(self) == typeof(rhs) == int else float
        return Infix(OP.ADD, self, rhs, dtype=dtype)

    def __mul__(self, rhs):
        dtype = int if typeof(self) == typeof(rhs) == int else float
        return Infix(OP.ADD, self, rhs, dtype=dtype)

    def __truediv__(self, rhs):
        dtype = int if typeof(self) == typeof(rhs) == int else float
        return Infix(OP.ADD, self, rhs, dtype=dtype)

    def contains(self, rhs):
        return Infix(OP.CONTAINS, self, rhs, dtype=bool)

    def ncontains(self, rhs):
        return Infix(OP.NCONTAINS, self, rhs, dtype=bool)

    def has(self, rhs):
        return Infix(OP.HAS, self, rhs, dtype=bool)

    def nhas(self, rhs):
        return Infix(OP.NHAS, self, rhs, dtype=bool)

    def isin(self, *args):
        in_list = ListLit(*args)
        return Infix(OP.IN, self, in_list, dtype=bool)

    def sum(self):
        """Aggregate the column by summation."""
        return Prefix(OP.SUM, self, agg=True, dtype=self.dtype)

    def avg(self):
        """Aggregate the column by averaging (arithmetic mean)."""
        return Prefix(OP.AVG, self, agg=True, dtype=float)

    def min(self):
        """Aggregate the column by taking the minimum value."""
        return Prefix(OP.MIN, self, agg=True, dtype=float)

    def max(self):
        """Aggregate the column by taking the maximum value."""
        return Prefix(OP.MAX, self, agg=True, dtype=float)

    def mean(self):
        """Aggregate the column by averaging (arithmetic mean). Alias for avg."""
        return self.avg()

    def percentile(self, *args):
        """Aggregate the column by calculating one or more percentiles."""
        return Prefix(OP.PERCENTILE, self, *args, agg=True, dtype=float)

    def dcount(self, accuracy=1):
        return Prefix(OP.DCOUNT, self, accuracy, agg=True, dtype=int)

    def bag_unpack(self):
        """Expand a dynamic property bag column into one column per property."""
        assert self.dtype in [dict, "dynamic"]
        return Prefix(OP.BAG_UNPACK, self)

    def asc(self):
        """Sort this column in ascending order."""
        self.ascending = True
        return self

    def desc(self):
        """Sort this column in descending order."""
        self.ascending = False
        return self

    def between(self, left, right):
        """Returns True if the column value is between left (inclusive) and right (inclusive)."""
        return Between(self, left, right)

    def nbetween(self, left, right):
        """Returns False if the column value is between left (inclusive) and right (inclusive)."""
        return Between(self, left, right, negate=True)

    def __getattr__(self, attr):
        """Access a field in a dynamic property bag."""
        if self.dtype in [dict, "dynamic"]:
            return Property(self, attr)
        raise AttributeError

    def __repr__(self):
        return f'Column("{self.name}", {self.dtype})'


class Evaluate:
    def __init__(self, expr):
        self.expr = expr

    def __str__(self):
        return f"| evaluate {str(self.expr)}"


def asc(expr):
    """Sort the column in ascending order.

    Parameters
    ----------
    expr:
        A column name string or Column expression instance.
    """
    if isinstance(expr, Column):
        expr.ascending = True
        return expr
    return Column(expr, typeof(expr), ascending=True)


def desc(expr):
    """Sort the column in descending order.

    Parameters
    ----------
    expr:
        A column name string or Column expression instance.
    """
    if isinstance(expr, Column):
        expr.ascending = False
        return expr
    return Column(expr, typeof(expr), asc=False)


class Order:
    def __init__(self, *args):
        self.args = args

    def __str__(self):
        args = []
        for arg in self.args:
            if isinstance(arg, Column):
                if arg.ascending:
                    args.append(quote(arg) + " asc")
                else:
                    args.append(quote(arg))
            else:
                args.append(arg)
        args_str = ",\n\t".join(args)
        return f"| order by\n\t{args_str}"


class Limit:
    def __init__(self, n):
        assert isinstance(n, int)
        self.n = n

    def __str__(self):
        return f"| limit {self.n}"


class Expand:
    def __init__(self, column):
        self.column = column

    def __str__(self):
        return f"| mv-expand {str(self.column)}"


class TableExpr:
    """A table or tabular expression."""

    def __init__(self, name, database, columns=None, ast=None):
        """A tabular expression.

        Parameters
        ----------
        name: str
            The name of the table in the database.
        database: KustoDatabase
            The database containing the table.
        columns: dict or list
            Either:
            1. A dictionary where keys are column names and values are
            data type names, or
            2. A list of Column instances.
        ast: list, default None
            Abstract syntax tree used internally to store a nested list of expressions.
        """
        self._ast = ast or []
        self.name = name
        self.database = database
        if columns is None:
            self.columns = {}
        elif isinstance(columns, (list, tuple)):
            self.columns = {c.name: c for c in columns}
        elif isinstance(columns, dict):
            self.columns = {k: Column(k, v) for k, v in columns.items()}
        else:
            raise ValueError("columns must be a dict or a list of Columns.")

    def __getattr__(self, name):
        try:
            return self.columns[name]
        except KeyError as exc:
            raise AttributeError from exc

    def __getitem__(self, name):
        return self.__getattr__(name)

    def project(self, *args, **kwargs):
        """Project (select) a list of columns.

        Parameters
        ----------
        args: list
            Column names to project.
        kwargs: dict
            Columns to project with renaming, where the key is the new name.
            Right hand side can be a Column or an expression.

        Returns
        -------
        A table expression.
        """
        renamed = {k: Column(k, typeof(v)) for k, v in kwargs.items()}
        columns = {k: v for k, v in self.columns.items() if k in args}
        columns = {**columns, **renamed}
        return TableExpr(
            self.name, self.database, columns=columns, ast=[*self._ast, Project(*args, **kwargs)]
        )

    def collect(self):
        """Compile the expression to a query, execute it, and return results."""
        query_str = str(self)
        return self.database.execute(query_str)

    def count(self):
        """Get the count of rows that would be returned by the expression."""
        return TableExpr(self.name, self.database, columns=self.columns, ast=[*self._ast, Count()])

    def distinct(self, *args):
        """Distinct values in the given column(s)."""
        return TableExpr(
            self.name, self.database, columns=self.columns, ast=[*self._ast, Distinct(*args)]
        )

    def where(self, *args):
        """Filter the expression by one or more predicates."""
        return TableExpr(
            self.name, self.database, columns=self.columns, ast=[*self._ast, Where(*args)]
        )

    def join(self, right, on, kind, *args, strategy=None):
        """Join this table expression to another.

        Parameters
        ----------
        right: TableExpr
            The table to join this table to.
        on: [str]
            The list of columns to join on.
        kind: str
            The kind of join. Options:
            - "inner"
            - "left"
            - "right"
            - "full"
            - "leftsemi"
            - "rightsemi"
            - "leftanti"
            - "rightanti"
        strategy: str, default None
            If "broadcast" then a broadcast join is used.
            If "shuffle" then a shuffle join is used.
            If another value or None, a single-node join strategy is used.
        """
        return TableExpr(
            self.name,
            self.database,
            columns=self.columns,
            ast=[*self._ast, Join(right, on, kind=kind, strategy=strategy)],
        )

    def summarize(self, by=None, shuffle=False, shufflekey=None, num_partitions=None, **kwargs):
        """Aggregate by columns.

        Parameters
        ----------
        by: list, default None
            List of Column instances or column name strings to group by.
        shuffle: bool, default False
            If True, `hint.strategy=shuffle` will be added to the Kusto query.
            The shufflekey parameter takes precedence; if it is not None, then
            shuffle will be ignored.
        shufflekey: [str], str, [Column], Column or bool, default None
            Indicates the key to be used for the shuffle summarize strategy. If
            a string or Column instance, or list thereof, is provided, these
            columns will be used as the shufflekey; `hint.shufflekey=foo, bar`
            will be added to the Kusto query.
        num_partitions: int, default None
            A query hint indicating the number of partitions to be used in the
            shuffle strategy. Has no effect unless `shuffle` or `shufflekey` is
            also provided.
        kwargs: Dict
            Aliased aggregation expressions, e.g. bar=foo.sum()
        """
        # Set table columns to those listed in by, args, kwargs
        if by is None:
            by = []
        elif isinstance(by, (str, Column)):
            by = [by]
        else:
            by = by
        by_cols = {}
        for col in by:
            if isinstance(col, str):
                col = self.columns[col]
            by_cols[col.name] = col

        kwarg_cols = {k: Column(k, typeof(v)) for k, v in kwargs.items()}
        columns = {**by_cols, **kwarg_cols}
        return TableExpr(
            self.name,
            self.database,
            columns=columns,
            ast=[
                *self._ast,
                Summarize(
                    by=by,
                    shuffle=shuffle,
                    shufflekey=shufflekey,
                    num_partitions=num_partitions,
                    **kwargs,
                ),
            ],
        )

    def extend(self, **kwargs):
        """Add new columns calculated from expressions.

        Parameters
        ----------
        kwargs: dict
            Aliased expressions, e.g. foo="bar", baz="quux"
        """
        new_cols = {}
        for key, val in kwargs.items():
            if key not in self.columns:
                new_cols[key] = Column(key, typeof(val))
        columns = {**self.columns, **new_cols}
        new_inst = TableExpr(
            self.name, self.database, columns=columns, ast=[*self._ast, Extend(**kwargs)]
        )
        return new_inst

    def order(self, *args):
        """Order the result set by the given columns.

        Parameters
        ----------
        args: array
            The columns to sort by.
        """
        return TableExpr(
            self.name, self.database, columns=self.columns, ast=[*self._ast, Order(*args)]
        )

    def sort(self, *args):
        """Order the result set by the given columns. Alias for .order().

        Parameters
        ----------
        args: array
            The columns to sort by.
        """
        return self.order(*args)

    def evaluate(self, expr):
        """Evaluate a Kusto plugin expression."""
        return TableExpr(
            self.name, self.database, columns=self.columns, ast=[*self._ast, Evaluate(expr)]
        )

    def limit(self, n):
        """Limit the result set to the first n rows.

        Parameters
        ----------
        n: int
            The number of rows to return.
        """
        return TableExpr(self.name, self.database, columns=self.columns, ast=[*self._ast, Limit(n)])

    def take(self, n):
        """Limit the result set to the first n rows. Alias for .limit().

        Parameters
        ----------
        n: int
            The number of rows to return.
        """
        return self.limit(n)

    def sample(self, n):
        """Randomly sample n rows from the dataset.

        Parameters
        ----------
        n: int
            The number of rows to sample.
        """
        return TableExpr(
            self.name, self.database, columns=self.columns, ast=[*self._ast, Sample(n)]
        )

    def sample_distinct(self, n, column):
        """Randomly sample n rows from the dataset with distinct values in column.

        Parameters
        ----------
        n: int
            The number of rows to sample.
        column:
            The column to sample distinct values from.
        """
        if isinstance(column, str):
            column = self.columns[column]
        return TableExpr(
            self.name,
            self.database,
            columns=self.columns,
            ast=[*self._ast, SampleDistinct(n, column)],
        )

    def mv_expand(self, column):
        """Expand a dynamic column into one row per value.

        Parameters
        ----------
        column: Column
            The column to expand. Must be a dynamic (or dict or array) column.
        """
        if isinstance(column, str):
            column = self.columns[column]
        return TableExpr(
            self.name,
            self.database,
            columns=self.columns,
            ast=[*self._ast, Expand(column)],
        )

    def __str__(self):
        ops = [
            f"cluster('{self.database.cluster}').database('{self.database.database}').['{self.name}']",
            *self._ast,
        ]
        query_str = "\n".join([str(op) for op in ops]) + "\n"
        return query_str


__all__ = ["TableExpr"]
