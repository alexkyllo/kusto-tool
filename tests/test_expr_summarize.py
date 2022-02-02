from kusto_tool.expression import Column, Summarize, TableExpr

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
    """TableExpr.summarize calls Summarize correctly"""
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
