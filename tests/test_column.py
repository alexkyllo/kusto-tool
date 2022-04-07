from kusto_tool.expression import Column


def test_column_contains():
    col = Column("foo", str)
    assert str(col.contains("bar")) == "foo contains 'bar'"


def test_column_has():
    col = Column("foo", str)
    assert str(col.has("bar")) == "foo has 'bar'"


def test_column_ncontains():
    assert str(Column("foo", str).ncontains("bar")) == "foo !contains 'bar'"


def test_column_nhas():
    assert str(Column("foo", str).nhas("bar")) == "foo !has 'bar'"


def test_between():
    assert str(Column("foo", int).between(1, 3)) == "foo between(1 .. 3)"


def test_nbetween():
    assert str(Column("foo", int).nbetween(1, 3)) == "foo !between(1 .. 3)"
