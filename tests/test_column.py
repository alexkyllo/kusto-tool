from kusto_tool.expression import Column


def test_column_contains():
    col = Column("foo", str)
    assert str(col.contains("bar")) == "foo contains 'bar'"


def test_column_has():
    col = Column("foo", str)
    assert str(col.has("bar")) == "foo has 'bar'"
