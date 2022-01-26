"""Classes for interacting with a Kusto database."""

from collections.abc import KeysView
from timeit import default_timer as timer

import jinja2 as jj
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from loguru import logger

from kusto_tool.expression import TableExpr


def list_to_kusto(lst):
    return "dynamic([\n\t'" + "',\n\t'".join(list(lst)) + "'\n])"


def dict_to_datatable(dictionary: dict) -> str:
    """Converts a dict to a Kusto datatable statement for use as a lookup table."""
    dict_str = "\n\t".join([f"'{k}', '{v}'," for k, v in dictionary.items()])
    template = """datatable(key: string, value: string)[
    {{ dict_str }}
]"""
    stmt = jj.Template(template).render(dict_str=dict_str)
    return stmt


def render_template_query(query, *args, **kwargs) -> str:
    """Render a query with optional parameters."""
    # Any list arguments need to be converted to Kusto list strings
    converted_kwargs = {
        k: list_to_kusto(v) if isinstance(v, (list, tuple, set, KeysView)) else v
        for k, v in kwargs.items()
    }
    return jj.Template(query).render(*args, **converted_kwargs)


def render_set(query, table, folder, docstring, replace=False, *args, **kwargs) -> str:
    """Render a .set-or-[append|replace] command from a query."""
    query_rendered = render_template_query(query, *args, **kwargs)
    command = "replace" if replace else "append"
    set_append_template = """.set-or-{{ command }} {{ table }}
with (
folder = "{{ folder }}",
docstring = "{{ docstring }}",
)
<|
{{ query_string }}
"""
    command_rendered = render_template_query(
        set_append_template,
        command=command,
        table=table,
        folder=folder,
        docstring=docstring,
        query_string=query_rendered,
    )
    return command_rendered


class KustoDatabase:
    """"""

    def __init__(self, server, database, client=None):
        """A class representing a Kusto database.

        Parameters
        ----------
        server: str
            The cluster name.
        database: str
            The database name.
        client: KustoClient, default None
            Pass this if you wish to provide your own KustoClient.
        """
        self.server = server
        self.server_uri = f"https://{server}.kusto.windows.net"
        self.database = database
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(self.server_uri)
        self.client = client or KustoClient(kcsb)

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

        Returns
        -------
        TableExpr: a table expression instance.
        """
        return TableExpr(name, database=self, columns=columns, inspect=inspect)

    def execute(self, query: str, *args, **kwargs):
        """Execute a query or command.

        Parameters
        ----------
        query: str
                The text of the Kusto query or commandto run.
        args: List[Any]
            Positional arguments to pass to the query as Jinja2 template params.
        kwargs: Dict[Any]
            Keyword arguments to pass to the query as Jinja2 template params.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the query results.
        """
        query_rendered = render_template_query(query, *args, **kwargs)
        if query_rendered.startswith("."):
            logger.info("Executing command on {}: {}", self.database, query_rendered)
            start_time = timer()
            result = self.client.execute_mgmt(self.database, query_rendered)
        else:
            start_time = timer()
            logger.info("Executing query on {}: {}", self.database, query_rendered)
            result = self.client.execute_query(self.database, query_rendered)
        end_time = timer()
        duration = end_time - start_time
        logger.info("Query execution completed in {:.2f} seconds.", duration)
        return dataframe_from_result_table(result.primary_results[0])

    def show_tables(self):
        """Show the list of tables in the database.

        Returns
        -------
        pandas.DataFrame: A DataFrame listing all tables in the database, in the
        column "TableName".
        """
        return self.execute(".show tables")

    def table_exists(self, table: str) -> bool:
        """Check if a table exists in the database.

        Parameters
        ----------
        table: str
            The name of the table to look for.

        Returns
        -------
        bool: True if the table exists in the database.
        """
        tables = self.show_tables()
        return table in tables.TableName.tolist()

    def set(self, query, table, folder, docstring, *args, **kwargs):
        """
        Runs your query and appends or replaces the results to a table.

        Parameters
        ----------
        query: str
            The text of the Kusto query to run.
        table: str
            The table name to create.
        folder: str
            The kusto folder to save the table into.
        docstring: str
            The docstring for the table metadata.
        replace: bool, default False.
            Appends to the table if True, replaces contents if False.
        args: List[Any]
            Positional arguments to pass to the query as Jinja2 template params.
        kwargs: Dict[Any]
            Keyword arguments to pass to the query as Jinja2 template params.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the results of the control command.
        """
        return self.execute(
            render_set(query, table, folder, docstring, replace=False, *args, **kwargs),
        )
