from kusto_tool.database import KustoDatabase
from kusto_tool.expression import TableExpr
from pytest import raises


def test_mv_expand():
    """mv-expand works"""
    tbl = TableExpr("tbl", KustoDatabase("c", "db"), columns=dict(foo=str))
    assert str(tbl.mv_expand(tbl.foo)) == "cluster('c').database('db').['tbl']\n| mv-expand foo\n"


def test_mv_expand_str():
    """mv-expand works"""
    tbl = TableExpr("tbl", KustoDatabase("c", "db"), columns=dict(foo=str))
    assert str(tbl.mv_expand("foo")) == "cluster('c').database('db').['tbl']\n| mv-expand foo\n"


def test_mv_expand_nonexistent_col():
    """mv-expand works"""
    tbl = TableExpr("tbl", KustoDatabase("c", "db"), columns=dict(foo=str))
    with raises(KeyError):
        tbl.mv_expand("bar")
