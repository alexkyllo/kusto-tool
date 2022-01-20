"""A class for interacting with a Kusto database."""


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
        return f"| project {column_list}\n"


class Column:
    """A column in a tabular expression."""

    def __init__(self, name: str, dtype: str):
        """"""
        self.name = name
        self.dtype = dtype

    def __str__(self):
        return self.name


class TableExpr:
    """A tabular expression."""

    def __init__(self, name, database, columns=None, inspect=False):
        """A tabular expression.

        Parameters
        ----------
        name: str
            The name of the table in the database.
        database: KustoDatabase
            The name of the database containing the table.
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
        # TODO: implemenct and call inspect if True
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

    def __str__(self):
        # TODO: recursively process AST
        query_str = (
            f"cluster('{self.database.server}').database('{self.database.database}').['{self.name}']\n"
            + "\n".join([str(op) for op in self._ast])
        )
        return query_str


class KustoDatabase:
    """"""

    def __init__(self, server, database):
        """"""
        self.server = server
        self.database = database

    def table(self, name, columns=None, inspect=False):
        """A tabular expression.

        Parameters
        ----------
        name: str
            The name of the table in the database.
        database: KustoDatabase
            The name of the database containing the table.
        columns: dict or list
            Either:
            1. A dictionary where keys are column names and values are
            data type names, or
            2. A list of Column instances.
        inspect: bool, default False
            If true, columns will be inspected from the database. If columns
            list is provided and inspect is true, inspect takes precedence.
        """
        return TableExpr(name, database=self, columns=columns, inspect=inspect)

    def query(self):
        """"""
        # TODO
