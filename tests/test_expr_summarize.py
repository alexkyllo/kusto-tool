from kusto_tool.expression import Column, Summarize

from .fake_database import FakeDatabase


def test_summarize_by():
    result = str(Summarize(by=["foo", "bar"]))
    expected = "| summarize\n\tby foo, bar"
    assert result == expected


def test_summarize_noby():
    result = str(Summarize(baz=Column("bar", int).sum()))
    expected = "| summarize\n\tbaz=sum(bar)"
    assert result == expected


def test_summarize_sum_by():
    result = str(Summarize(baz=Column("bar", int).sum(), by="foo"))
    expected = "| summarize\n\tbaz=sum(bar)\n\tby foo"
    assert result == expected


def test_summarize_strategy():
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
