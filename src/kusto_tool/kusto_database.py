"""A class for interacting with a Kusto database."""


class Project:
    def __init__(self, *args, **kwargs):
        self.columns = list(args)
        self.renamed_columns = kwargs

    def _build_column_list(self):
        col_list = self.columns
        for k, v in self.renamed_columns.items():
            col_list.append(f"{k} = {v}")
        col_str = ",\n".join(col_list)
        return col_str

    def __str__(self):
        column_list = self._build_column_list()
        return f"| project {column_list}\n"


class TableExpr:
    """"""

    def __init__(self, name, database):
        self.name = name
        self._ast = []
        self.database = database

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
        query_str = f"['{self.name}']\n" + "\n".join([str(op) for op in self._ast])
        return query_str


class KustoDatabase:
    """"""

    def __init__(self, server, database):
        """"""

    def table(self, name):
        """"""
        return TableExpr(name, database=self)

    def query(self):
        """"""
        # TODO
