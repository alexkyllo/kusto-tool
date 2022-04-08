from kusto_tool.database import KustoDatabase
from kusto_tool.expression import Column, Order, TableExpr, asc


def test_order_1arg():
    """order by works"""
    assert str(Order(Column("foo", str))) == "| order by\n\tfoo"


def test_order_2args():
    """order by works with 2 args"""
    assert str(Order(Column("foo", str), Column("bar", str))) == "| order by\n\tfoo,\n\tbar"


def test_order_asc():
    """order works with asc"""
    col = Column("foo", str).asc()
    expr = Order(col)
    assert str(expr) == "| order by\n\tfoo asc"


def test_order_asc_multiple():
    """order works with asc"""
    expr = Order(Column("foo", str).desc(), Column("bar", str).asc(), Column("baz", str).desc())
    assert str(expr) == "| order by\n\tfoo,\n\tbar asc,\n\tbaz"


def test_order_tbl():
    """order by works with a table"""
    tbl = TableExpr("tbl", KustoDatabase("cluster", "db"), columns={"foo": str})
    assert (
        str(tbl.order(tbl.foo)) == "cluster('cluster').database('db').['tbl']\n| order by\n\tfoo\n"
    )


def test_order_tbl_asc():
    """order by works with a table"""
    tbl = TableExpr("tbl", KustoDatabase("cluster", "db"), columns={"foo": str})
    assert (
        str(tbl.order(tbl.foo.asc()))
        == "cluster('cluster').database('db').['tbl']\n| order by\n\tfoo asc\n"
    )


def test_sort_tbl():
    """sort alias works works with a table"""
    tbl = TableExpr("tbl", KustoDatabase("cluster", "db"), columns={"foo": str})
    assert (
        str(tbl.sort(tbl.foo)) == "cluster('cluster').database('db').['tbl']\n| order by\n\tfoo\n"
    )


def test_order_str():
    """order works with a string column name"""
    assert str(Order("foo")) == "| order by\n\tfoo"


def test_order_str_asc():
    """order works with a string column name"""
    assert str(Order(asc("foo"))) == "| order by\n\tfoo asc"
