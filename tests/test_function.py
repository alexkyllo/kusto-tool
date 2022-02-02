import kusto_tool.function as F
from kusto_tool.expression import Column, Summarize, TableExpr

from .fake_database import FakeDatabase


def test_strcat():
    """strcat translates correctly"""
    assert str(F.strcat(Column("foo", str), "_", "bar")) == "strcat(foo, '_', 'bar')"


def test_dcount_str():
    """dcount works correctly for a string arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(dcount_foo=F.dcount("foo", 2))
    assert "dcount_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tdcount_foo=dcount(foo, 2)
"""
    assert result == expected


def test_dcount_col():
    """dcount works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(dcount_foo=tbl.foo.dcount(2))
    assert "dcount_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tdcount_foo=dcount(foo, 2)
"""
    assert result == expected


def test_avg_col():
    """avg works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(avg_foo=tbl.foo.avg())
    assert "avg_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tavg_foo=avg(foo)
"""
    assert result == expected


def test_avg_function_col():
    """avg works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(avg_foo=F.avg(tbl.foo))
    assert "avg_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tavg_foo=avg(foo)
"""
    assert result == expected


def test_avg_function_str():
    """avg works correctly for a str arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(avg_foo=F.avg("foo"))
    assert "avg_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tavg_foo=avg(foo)
"""
    assert result == expected


def test_mean_function_col():
    """avg works correctly for a col arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(avg_foo=F.mean(tbl.foo))
    assert "avg_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tavg_foo=avg(foo)
"""
    assert result == expected


def test_mean_column():
    """avg works correctly."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(avg_foo=tbl.foo.mean())
    assert "avg_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tavg_foo=avg(foo)
"""
    assert result == expected


def test_count():
    """avg works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(ct=F.count())
    assert "ct" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tct=count()
"""
    assert result == expected
