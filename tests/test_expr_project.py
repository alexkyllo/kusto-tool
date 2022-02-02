from kusto_tool.database import KustoDatabase
from kusto_tool.expression import Column, Property, TableExpr
from pytest import raises


def test_project_rename_col():
    tbl = TableExpr("tbl", KustoDatabase("test", "testdb"), columns={"foo": int})
    expr = tbl.project(bar=tbl.foo)
    assert expr.columns["bar"].dtype == int
    assert "foo" not in expr.columns
    query = str(expr)
    expected = "cluster('test').database('testdb').['tbl']\n| project\n\tbar = foo\n"
    assert query == expected


def test_project_math_expr():
    tbl = TableExpr("tbl", KustoDatabase("test", "testdb"), columns={"foo": int})
    expr = tbl.project(bar=tbl.foo + 1)
    assert expr.columns["bar"].dtype == int
    assert "foo" not in expr.columns
    query = str(expr)
    expected = "cluster('test').database('testdb').['tbl']\n| project\n\tbar = foo + 1\n"
    assert query == expected


def test_property_access_table():
    """Property access succeeds for dict"""
    tbl = TableExpr("tbl", KustoDatabase("test", "testdb"), columns={"foo": dict})
    expr = tbl.project(bar=tbl.foo.baz)
    query = str(expr)
    assert query == "cluster('test').database('testdb').['tbl']\n| project\n\tbar = foo.baz\n"


def test_property_access():
    """Property access succeeds for dict"""
    assert str(Property(Column("foo", dict), "bar")) == "foo.bar"


def test_property_access_wrong_dtype():
    """Property access errors for non-dynamic column"""
    tbl = TableExpr("tbl", KustoDatabase("test", "testdb"), columns={"foo": str})
    with raises(AttributeError):
        expr = tbl.project(bar=tbl.foo.baz)
