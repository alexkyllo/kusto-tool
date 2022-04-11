from datetime import datetime

import kusto_tool.function as F
from kusto_tool.expression import Column, Summarize, TableExpr

from .fake_database import FakeDatabase


def test_function():
    """any function can be translated to Kusto"""
    assert str(F.function("wiggle", 4, "foo")) == "wiggle(4, 'foo')"


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


def test_startofday():
    """startofday works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(daystart=F.startofday(tbl.foo))
    assert "daystart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tdaystart = startofday(foo)
"""
    assert result == expected


def test_startofday_offset():
    """startofday works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(daystart=F.startofday(tbl.foo, 1))
    assert "daystart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tdaystart = startofday(foo, 1)
"""
    assert result == expected


def test_endofday():
    """startofday works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(daystart=F.endofday(tbl.foo))
    assert "daystart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tdaystart = endofday(foo)
"""
    assert result == expected


def test_endofday_offset():
    """startofday works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(daystart=F.endofday(tbl.foo, 1))
    assert "daystart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tdaystart = endofday(foo, 1)
"""
    assert result == expected


def test_startofweek():
    """startofweek works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(weekstart=F.startofweek(tbl.foo))
    assert "weekstart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tweekstart = startofweek(foo)
"""
    assert result == expected


def test_startofweek_offset():
    """startofweek works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(weekstart=F.startofweek(tbl.foo, 1))
    assert "weekstart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tweekstart = startofweek(foo, 1)
"""
    assert result == expected


def test_endofweek():
    """endofweek works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(weekend=F.endofweek(tbl.foo))
    assert "weekend" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tweekend = endofweek(foo)
"""
    assert result == expected


def test_endofweek_offset():
    """endofweek works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(weekend=F.endofweek(tbl.foo, 1))
    assert "weekend" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tweekend = endofweek(foo, 1)
"""
    assert result == expected


def test_startofmonth():
    """startofmonth works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(monthstart=F.startofmonth(tbl.foo))
    assert "monthstart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tmonthstart = startofmonth(foo)
"""
    assert result == expected


def test_startofmonth_offset():
    """startofmonth works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(monthstart=F.startofmonth(tbl.foo, 1))
    assert "monthstart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tmonthstart = startofmonth(foo, 1)
"""
    assert result == expected


def test_endofmonth():
    """endofmonth works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(monthend=F.endofmonth(tbl.foo))
    assert "monthend" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tmonthend = endofmonth(foo)
"""
    assert result == expected


def test_endofmonth_offset():
    """endofmonth works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(monthend=F.endofmonth(tbl.foo, 1))
    assert "monthend" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tmonthend = endofmonth(foo, 1)
"""
    assert result == expected


def test_startofyear():
    """startofyear works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(yearstart=F.startofyear(tbl.foo))
    assert "yearstart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tyearstart = startofyear(foo)
"""
    assert result == expected


def test_startofyear_offset():
    """startofyear works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(yearstart=F.startofyear(tbl.foo, 1))
    assert "yearstart" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tyearstart = startofyear(foo, 1)
"""
    assert result == expected


def test_endofyear():
    """endofyear works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(yearend=F.endofyear(tbl.foo))
    assert "yearend" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tyearend = endofyear(foo)
"""
    assert result == expected


def test_endofyear_offset():
    """endofyear works correctly for a column arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": datetime})
    query = tbl.project(yearend=F.endofyear(tbl.foo, 1))
    assert "yearend" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| project
\tyearend = endofyear(foo, 1)
"""
    assert result == expected
