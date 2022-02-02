from kusto_tool.expression import Column, Summarize, TableExpr
from kusto_tool.function import strcat, sum
from pytest import raises

from .fake_database import FakeDatabase


def test_summarize_by_list_str():
    """by clause can be included in summarize as a list of strings"""
    result = str(Summarize(by=["foo", "bar"]))
    expected = "| summarize\n\tby foo, bar"
    assert result == expected


def test_summarize_by_str():
    """by clause can be included in summarize as a single string"""
    result = str(Summarize(by="foo"))
    expected = "| summarize\n\tby foo"
    assert result == expected


def test_summarize_by_list_col():
    """by clause can be included in summarize as a list of columns"""
    result = str(Summarize(by=[Column("foo", str), Column("bar", str)]))
    expected = "| summarize\n\tby foo, bar"
    assert result == expected


def test_summarize_by_col():
    """by clause can be included in summarize as a single Column"""
    result = str(Summarize(by=Column("foo", str)))
    expected = "| summarize\n\tby foo"
    assert result == expected


def test_summarize_noby():
    """by clause can be omitted from summarize"""
    result = str(Summarize(baz=Column("bar", int).sum()))
    expected = "| summarize\n\tbaz=sum(bar)"
    assert result == expected


def test_summarize_sum_by():
    """summarize can use sum()"""
    result = str(Summarize(baz=Column("bar", int).sum(), by="foo"))
    expected = "| summarize\n\tbaz=sum(bar)\n\tby foo"
    assert result == expected


def test_summarize_sum_two_cols():
    """Summarize can work with two columns"""
    result = str(
        Summarize(baz=Column("bar", int).sum(), quux=Column("bar", int).dcount(), by="foo")
    )
    expected = "| summarize\n\tbaz=sum(bar),\n\tquux=dcount(bar, 1)\n\tby foo"
    assert result == expected


def test_summarize_dcount_by():
    """summarize can use sum()"""
    result = str(Summarize(baz=Column("bar", int).dcount(), by="foo"))
    expected = "| summarize\n\tbaz=dcount(bar, 1)\n\tby foo"
    assert result == expected


def test_summarize_strategy():
    """shuffle parameter works"""
    result = str(Summarize(baz=Column("bar", int).sum(), by="foo", shuffle=True))
    expected = "| summarize hint.strategy=shuffle\n\tbaz=sum(bar)\n\tby foo"
    assert result == expected


def test_summarize_strategy_shufflekey_list_col():
    """shufflekey parameter works for list of columns"""
    result = str(
        Summarize(
            baz=Column("bar", int).sum(),
            by="foo",
            shufflekey=[Column("baz", str), Column("quux", str)],
        )
    )
    expected = "| summarize hint.shufflekey=baz, quux\n\tbaz=sum(bar)\n\tby foo"
    assert result == expected


def test_summarize_strategy_shufflekey_list_str():
    """shufflekey parameter works for list of strings"""
    result = str(Summarize(baz=Column("bar", int).sum(), by="foo", shufflekey=["baz", "quux"]))
    expected = "| summarize hint.shufflekey=baz, quux\n\tbaz=sum(bar)\n\tby foo"
    assert result == expected


def test_summarize_strategy_shufflekey_col():
    """shufflekey parameter works for as single column"""
    result = str(Summarize(baz=Column("bar", int).sum(), by="foo", shufflekey=Column("baz", str)))
    expected = "| summarize hint.shufflekey=baz\n\tbaz=sum(bar)\n\tby foo"
    assert result == expected


def test_summarize_strategy_shufflekey_str():
    """shufflekey parameter works for a single string"""
    result = str(Summarize(baz=Column("bar", int).sum(), by="foo", shufflekey="baz"))
    expected = "| summarize hint.shufflekey=baz\n\tbaz=sum(bar)\n\tby foo"
    assert result == expected


def test_summarize_strategy_partitions():
    """num_partitions parameter works"""
    result = str(
        Summarize(
            baz=Column("bar", int).sum(),
            by="foo",
            shufflekey=[Column("baz", str), Column("quux", str)],
            num_partitions=10,
        )
    )
    expected = (
        "| summarize hint.shufflekey=baz, quux hint.num_partitions=10\n\tbaz=sum(bar)\n\tby foo"
    )
    assert result == expected


def test_summarize_strategy_partitions_ignored():
    """num_partitions is ignored if not(shuffle or shufflekey)"""
    result = str(
        Summarize(
            baz=Column("bar", int).sum(),
            by="foo",
            num_partitions=10,
        )
    )
    expected = "| summarize\n\tbaz=sum(bar)\n\tby foo"
    assert result == expected


def test_tableexpr_summarize():
    """TableExpr.summarize calls Summarize correctly"""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(sum_foo=Column("foo", str).sum(), by="bar")
    assert "sum_foo" in query.columns
    assert "bar" in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tsum_foo=sum(foo)
\tby bar
"""
    assert result == expected


def test_tableexpr_summarize_noby():
    """TableExpr.summarize calls Summarize correctly without a by clause"""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(sum_foo=Column("foo", str).sum())
    assert "sum_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tsum_foo=sum(foo)
"""
    assert result == expected


def test_tableexpr_summarize_function():
    """TableExpr.summarize calls Summarize correctly with the sum() function."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(sum_foo=sum(Column("foo", str)))
    assert "sum_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tsum_foo=sum(foo)
"""
    assert result == expected


def test_tableexpr_summarize_function_str():
    """TableExpr.summarize calls Summarize correctly for a string arg."""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(sum_foo=sum("foo"))
    assert "sum_foo" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tsum_foo=sum(foo)
"""
    assert result == expected


def test_tableexpr_summarize_function_missing_col():
    """TableExpr.summarize calls Summarize correctly"""
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    with raises(AttributeError):
        tbl.summarize(sum_foo=tbl.baz.sum())


def test_tableexpr_summarize_function_str_missing_col():
    """TableExpr.summarize calls Summarize correctly for a nonexisting string arg.
    Note: This query will fail on the server side since we cannot tell if "baz"
    is a real column in the dataset.
    """
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.summarize(sum_baz=sum("baz"))
    assert "sum_baz" in query.columns
    assert "bar" not in query.columns
    assert "foo" not in query.columns
    result = str(query)
    expected = """cluster('help').database('Samples').['tbl']
| summarize
\tsum_baz=sum(baz)
"""
    assert result == expected


def test_summarize_nonagg_fails():
    db = FakeDatabase("help", "Samples")
    tbl = TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    with raises(AssertionError):
        tbl.summarize(sum_foo=strcat(Column("foo", str)))
