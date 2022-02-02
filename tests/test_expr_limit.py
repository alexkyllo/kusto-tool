from kusto_tool.database import KustoDatabase
from kusto_tool.expression import Limit, TableExpr


def test_limit():
    assert str(Limit(1000)) == "| limit 1000"


def test_limit_tbl():
    assert (
        str(TableExpr("tbl", KustoDatabase("c", "db")).limit(1000))
        == "cluster('c').database('db').['tbl']\n| limit 1000\n"
    )


def test_take():
    assert (
        str(TableExpr("tbl", KustoDatabase("c", "db")).take(1000))
        == "cluster('c').database('db').['tbl']\n| limit 1000\n"
    )
