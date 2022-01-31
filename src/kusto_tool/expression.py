"""Experimental Kusto expression API for generating queries."""


class attrdict:
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
    AND="and",
    OR="or",
    NOT="not",
)


def quote(val):
    if isinstance(val, str):
        return f"'{val}'"
    return str(val)


class UnaryExpression:
    """"""

    def __init__(self, op, *args, agg=False):
        self.terms = args
        self.op = op
        self.agg = agg

    def __str__(self):
        terms = ", ".join([quote(term) for term in self.terms])
        return f"{self.op}({terms})"


class BinaryExpression:
    def __init__(self, op, lhs, rhs):
        self.op = op
        self.lhs = lhs
        self.rhs = rhs

    def __str__(self):
        if self.op in [OP.AND, OP.OR]:
            return f"({self.lhs}) {self.op} ({quote(self.rhs)})"
        return f"{self.lhs} {self.op} {quote(self.rhs)}"

    def __repr__(self):
        return f"{repr(self.lhs)} {self.op} {quote(self.rhs)}"

    def __and__(self, rhs):
        return BinaryExpression(OP.AND, self, rhs)

    def __or__(self, rhs):
        return BinaryExpression(OP.OR, self, rhs)

    def __invert__(self):
        return UnaryExpression(OP.NOT, self)


class Project:
    def __init__(self, *args, **kwargs):
        self.columns = list(args)
        self.renamed_columns = kwargs

    def _build_column_list(self):
        col_list = self.columns
        for k, v in self.renamed_columns.items():
            col_list.append(f"{k} = {v}")
        col_str = ",\n".join([str(col) for col in col_list])
        return col_str

    def __str__(self):
        column_list = self._build_column_list()
        return f"| project {column_list}"


class Count:
    def __repr__(self):
        return "Count()"

    def __str__(self):
        return "| count"


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
    def __init__(
        self, *args, by=None, shuffle=False, shufflekey=None, num_partitions=None, **kwargs
    ):
        converted_args = {f"{str(v.op)}_{str(v.term)}": v for v in args}
        self.expressions = {**converted_args, **kwargs}
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
            expr_list = ",".join([f"{str(k)}={str(v)}" for k, v in self.expressions.items()])
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


class Column:
    """A column in a tabular expression."""

    def __init__(self, name: str, dtype: str):
        """"""
        self.name = name
        self.dtype = dtype

    def __str__(self):
        return self.name

    def __eq__(self, rhs):
        return BinaryExpression(OP.EQ, self, rhs)

    def __ne__(self, rhs):
        return BinaryExpression(OP.NE, self, rhs)

    def __lt__(self, rhs):
        return BinaryExpression(OP.LT, self, rhs)

    def __le__(self, rhs):
        return BinaryExpression(OP.LE, self, rhs)

    def __gt__(self, rhs):
        return BinaryExpression(OP.GT, self, rhs)

    def __ge__(self, rhs):
        return BinaryExpression(OP.GE, self, rhs)

    def contains(self, rhs):
        return BinaryExpression(OP.CONTAINS, self, rhs)

    def ncontains(self, rhs):
        return BinaryExpression(OP.NCONTAINS, self, rhs)

    def has(self, rhs):
        return BinaryExpression(OP.HAS, self, rhs)

    def nhas(self, rhs):
        return BinaryExpression(OP.NHAS, self, rhs)

    def sum(self):
        return UnaryExpression(OP.SUM, self)

    def __repr__(self):
        return f'Column("{self.name}", {self.dtype})'


class TableExpr:
    """A tabular expression."""

    def __init__(self, name, database, columns=None, inspect=False):
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
        inspect: bool, default False
            If true, columns will be inspected from the database. If columns
            list is provided and inspect is true, inspect takes precedence.
        """
        self._ast = []
        self.name = name
        self.database = database
        # TODO: implement and call inspect() to get schema metadata if True
        if columns is None:
            self.columns = {}
        elif isinstance(columns, (list, tuple)):
            self.columns = {c.name: c for c in columns}
        elif isinstance(columns, dict):
            self.columns = {k: Column(k, v) for k, v in columns.items()}
        else:
            raise ValueError("columns must be a dict or a list of Columns.")
        self.inspect = inspect

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
            Column names to project with renaming, where the key is the new name.

        Returns
        -------
        A table expression.
        """
        self._ast.append(Project(*args, **kwargs))
        return self

    def collect(self):
        """"""
        query_str = str(self)
        return self.database.query(query_str)

    def count(self):
        self._ast.append(Count())
        return self

    def distinct(self, *args):
        self._ast.append(Distinct(*args))
        return self

    def where(self, *args):
        self._ast.append(Where(*args))
        return self

    def join(self, right, on, kind, strategy=None, *args):
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
        self._ast.append(Join(right, on, kind=kind, strategy=strategy))
        return self

    def summarize(
        self, *args, by=None, shuffle=False, shufflekey=None, num_partitions=None, **kwargs
    ):
        """Aggregate by columns.

        Parameters
        ----------
        args: list
            Un-aliased expressions, e.g. foo.sum().
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
            Aliased expressions, e.g. bar=foo.sum()
        """
        self._ast.append(
            Summarize(
                *args,
                by=by,
                shuffle=shuffle,
                shufflekey=shufflekey,
                num_partitions=num_partitions,
                **kwargs,
            )
        )
        return self

    def __str__(self):
        # TODO: recursively process AST
        ops = [
            f"cluster('{self.database.cluster}').database('{self.database.database}').['{self.name}']",
            *self._ast,
        ]
        query_str = "\n".join([str(op) for op in ops]) + "\n"
        return query_str
