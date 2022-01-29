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
    print(result)
    print(expected)
    assert result == expected
