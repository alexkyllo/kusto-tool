"""Classes for interacting with a Kusto database."""
import os
from collections.abc import KeysView
from pathlib import Path
from timeit import default_timer as timer

import jinja2 as jj
import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from loguru import logger

from kusto_tool.expression import KTYPES, TableExpr, quote


def list_to_kusto(lst):
    """Convert a Python list to a Kusto list literal."""
    list_str = [quote(i) for i in list(lst)]
    return "dynamic([\n\t" + ",\n\t".join(list_str) + "\n])"


def dict_to_datatable(dictionary: dict) -> str:
    """Converts a dict to a Kusto datatable statement for use as a lookup table."""
    dict_str = "\n\t".join([f"'{k}', '{v}'," for k, v in dictionary.items()])
    template = """datatable(key: string, value: string)[
    {{ dict_str }}
]"""
    stmt = jj.Template(template).render(dict_str=dict_str)
    return stmt


def maybe_read_file(query):
    """Read contents from a file if the argument is a file path."""
    if os.path.isfile(query):
        logger.info("Reading file {}.", query)
        with open(query, "r", encoding="utf-8") as query_file:
            return query_file.read()
    return query


def render_template_query(query, *args, **kwargs) -> str:
    """Render a query with optional parameters."""
    # Any list arguments need to be converted to Kusto list strings
    query = maybe_read_file(query)
    converted_kwargs = {
        k: list_to_kusto(v) if isinstance(v, (list, tuple, set, KeysView)) else v
        for k, v in kwargs.items()
    }
    return jj.Template(query).render(*args, **converted_kwargs)


def render_set(query, table, folder, docstring, *args, replace=False, **kwargs) -> str:
    """Render a .set-or-[append|replace] command from a query."""
    query = maybe_read_file(query)
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
    """A class representing a Kusto database."""

    def __init__(self, cluster, database, client=None):
        """A class representing a Kusto database.

        Parameters
        ----------
        cluster: str
            The cluster name.
        database: str
            The database name.
        client: KustoClient, default None
            Pass this if you wish to provide your own KustoClient.
        """
        self.cluster = cluster
        self.cluster_uri = f"https://{cluster}.kusto.windows.net"
        self.database = database
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(self.cluster_uri)
        self.client = client or KustoClient(kcsb)

    def table(self, name, columns=None, inspect=False):
        """A tabular expression.

        Parameters
        ----------
        name: str
            The name of the table in the database.
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
        TableExpr
            A table expression instance.
        """
        if inspect:
            columns = self.execute(f".show table {name} cslschema").Schema
            if len(columns) < 1:
                raise KeyError(f"Table {name} does not exist in the database.")
            columns = columns.item()
            columns = columns.split(",")
            columns = {col.split(":")[0]: KTYPES[col.split(":")[1]] for col in columns}
        return TableExpr(name, database=self, columns=columns)

    def execute(self, query: str, *args, **kwargs):
        """Execute a query or command.

        Parameters
        ----------
        query: str
            The text of the Kusto query or command to run. Can also be a path to
            a file containing a query.
        args: List[Any]
            Positional arguments to pass to the query as Jinja2 template params.
        kwargs: Dict[Any]
            Keyword arguments to pass to the query as Jinja2 template params.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the query results.
        """
        query = maybe_read_file(query)
        query_rendered = render_template_query(query, *args, **kwargs)

        method = (
            self.client.execute_mgmt
            if query_rendered.startswith(".")
            else self.client.execute_query
        )
        logger.info("Executing query on {}: {}", self.database, query_rendered)
        start_time = timer()
        result = method(self.database, query_rendered)
        end_time = timer()
        duration = end_time - start_time
        logger.info("Query execution completed in {:.2f} seconds.", duration)
        return dataframe_from_result_table(result.primary_results[0])

    def show_tables(self):
        """Show the list of tables in the database.

        Returns
        -------
        pandas.DataFrame
            A DataFrame listing all tables in the database, in the column "TableName".
        """
        return self.execute(".show tables")

    def drop_table(self, table):
        """Drop a table from the database.

        Parameters
        ----------
        table: str
            The name of the table to drop.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the name of the dropped table, indicating success.
        """
        return self.execute(f".drop table {table}")

    def table_exists(self, table: str) -> bool:
        """Check if a table exists in the database.

        Parameters
        ----------
        table: str
            The name of the table to look for.

        Returns
        -------
        bool
            True if the table exists in the database.
        """
        tables = self.show_tables()
        return table in tables.TableName.tolist()

    def set_table(self, query, table, folder, docstring, *args, **kwargs):
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

    def to_parquet(self, query, path, *args, force=False, **kwargs):
        """Run the given query, cache the results as a local parquet file, and
        return results as a Pandas DataFrame.

        Parameters
        ----------
        query: str
            The text of the Kusto query to run.
        path: str
            The path to save the parquet file.
        force: bool, default False
            If False, the data will be read from the cached parquet file if it
            exists. If True, the data will be re-downloaded regardless.
        args: List[Any]
            Positional arguments to pass to the query as Jinja2 template params.
        kwargs: Dict[Any]
            Keyword arguments to pass to the query as Jinja2 template params.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the results of the control command.
        """
        if not force:
            if os.path.isfile(path):
                logger.info("Reading dataframe from existing file {}", path)
                return pd.read_parquet(path)
        logger.info("File {} does not exist, will run query.", path)
        df = self.execute(query, *args, **kwargs)
        df.to_parquet(path, index=False)
        return df

    def __str__(self):
        return f"{str(self.cluster)}.database('{self.database}')"


class Cluster:
    """A class representing a Kusto cluster."""

    def __init__(self, name):
        self.name = name

    def database(self, name):
        """Create an instance representing a database in the cluster.

        Parameters
        ----------
        name: str
            The name of the Kusto database in the cluster.

        Returns
        -------
        KustoDatabase
            an instance representing the Kusto database.
        """
        return KustoDatabase(self.name, name)

    def __str__(self):
        return f"cluster('{self.name}')"


def cluster(name):
    """Convenience function to construct a Cluster instance.
    Makes the query look more like KQL.
    """
    return Cluster(name)
