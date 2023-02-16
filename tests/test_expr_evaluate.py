import kusto_tool.function as F
from kusto_tool.database import KustoDatabase
from kusto_tool.expression import Evaluate, TableExpr
from pytest import raises


def test_evaluate():
    """evaluate bag_unpack works"""
    tbl = TableExpr("tbl", KustoDatabase("cluster", "db"), columns={"foo": dict})
    expr = Evaluate(tbl.foo.bag_unpack())
    assert str(expr) == "| evaluate bag_unpack(foo)"


def test_evaluate_bag_unpack():
    """evaluate bag_unpack works when called on Column"""
    tbl = TableExpr("tbl", KustoDatabase("cluster", "db"), columns={"foo": "dynamic"})
    expr = tbl.evaluate(tbl.foo.bag_unpack())
    assert str(expr) == "cluster('cluster').database('db').['tbl']\n| evaluate bag_unpack(foo)\n"


def test_evaluate_bag_unpack_wrong_dtype():
    """evaluate bag_unpack errors on wrong data type"""
    tbl = TableExpr("tbl", KustoDatabase("cluster", "db"), columns={"foo": str})
    with raises(AssertionError):
        tbl.evaluate(tbl.foo.bag_unpack())


def test_evaluate_sql_request():
    sql = "SELECT * FROM \"mytable\" WHERE Date = '2022-02-14'"
    tbl = TableExpr("tbl", KustoDatabase("cluster", "db"), columns={"foo": str})
    expr = tbl.evaluate(
        F.function(
            "sql_request",
            "Server=tcp:contoso.database.windows.net,1433;"
            'Authentication="Active Directory Integrated";'
            "Initial Catalog=Fabrikam;",
            sql,
        )
    )
    expected = (
        "cluster('cluster').database('db').['tbl']\n"
        "| evaluate sql_request('Server=tcp:contoso.database.windows.net,1433;Authentication="
        '"Active Directory Integrated";Initial Catalog=Fabrikam;\', '
        "'SELECT * FROM \"mytable\" WHERE Date = \\'2022-02-14\\'')\n"
    )
    assert str(expr) == expected
