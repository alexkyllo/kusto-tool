"""Test fakes."""
from azure.kusto.data.response import KustoResponseDataSetV2, KustoResultTable


class FakeDatabase:
    """Fake database for testing."""

    def __init__(self, cluster, database):
        self.cluster = cluster
        self.database = database

    def execute(self, query):
        """Just return the query instead of running it."""
        return query


class FakeKustoResponseDataSet(KustoResponseDataSetV2):
    def __init__(self, tables):
        self.tables = tables
        self.query = None

    @property
    def primary_results(self):
        return self.tables


class FakeKustoResultTable(KustoResultTable):
    def __init__(self, columns, rows):
        self.columns = columns
        self.raw_rows = rows


class FakeKustoClient:
    """Fake KustoClient for testing."""

    def __init__(self, table):
        self.response = FakeKustoResponseDataSet([FakeKustoResultTable(table)])

    def execute_mgmt(self, database, query):
        self.response.query = query
        return self.response

    def execute_query(self, database, query):
        self.response.query = query
        return self.response
