import kusto_tool.function as F
from kusto_tool.database import KustoDatabase
from kusto_tool.expression import Evaluate, TableExpr


def test_evaluate():
    tbl = TableExpr("tbl", KustoDatabase("cluster", "db"), columns={"foo": str})
    expr = Evaluate(tbl.foo.bag_unpack())
    assert str(expr) == "| evaluate bag_unpack(foo)"


def test_evaluate_table():
    tbl = TableExpr("tbl", KustoDatabase("cluster", "db"), columns={"foo": str})
    expr = tbl.evaluate(tbl.foo.bag_unpack())
    assert str(expr) == "cluster('cluster').database('db').['tbl']\n| evaluate bag_unpack(foo)\n"
