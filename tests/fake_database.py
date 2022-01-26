import pandas as pd
from azure.kusto.data._models import KustoResultColumn
from azure.kusto.data.response import KustoResponseDataSetV2, KustoResultTable


class FakeDatabase:
    """Fake database for testing."""

    def __init__(self, server, database):
        self.server = server
        self.database = database

    def query(self, query):
        """Just return the query instead of running it."""
        return query


class FakeKustoResponseDataSet(KustoResponseDataSetV2):
    """"""

    def __init__(self, tables):
        self.tables = tables

    @property
    def primary_results(self):
        return self.tables


class FakeKustoResultTable(KustoResultTable):
    def __init__(self):
        self.columns = [
            KustoResultColumn({"ColumnName": "fake_column", "ColumnType": "string"}, ordinal=0),
            KustoResultColumn({"ColumnName": "row_number", "ColumnType": "int"}, ordinal=1),
        ]
        self.raw_rows = [
            [
                "fake data",
                1,
            ],
            [
                "fake data",
                2,
            ],
        ]


class FakeKustoClient:
    """Fake KustoClient for testing."""

    def execute_mgmt(self, database, query):
        res = FakeKustoResponseDataSet([FakeKustoResultTable()])
        return res

    def execute_query(self, database, query):
        return self.execute_mgmt(database, query)
