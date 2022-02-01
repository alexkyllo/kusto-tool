from kusto_tool.database import KustoDatabase
from kusto_tool.expression import Project, TableExpr


def test_project_rename_col():
    tbl = TableExpr("tbl", KustoDatabase("test", "testdb"), columns={"foo": int})
    expr = tbl.project(bar=tbl.foo)
    assert expr.columns["bar"].dtype == int
    assert "foo" not in expr.columns
    query = str(expr)
    expected = "cluster('test').database('testdb').['tbl']\n| project bar = foo\n"
    assert query == expected


def test_project_math_expr():
    tbl = TableExpr("tbl", KustoDatabase("test", "testdb"), columns={"foo": int})
    expr = tbl.project(bar=tbl.foo + 1)
    assert expr.columns["bar"].dtype == int
    assert "foo" not in expr.columns
    query = str(expr)
    expected = "cluster('test').database('testdb').['tbl']\n| project bar = foo + 1\n"
    assert query == expected
