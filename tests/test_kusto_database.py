from kusto_tool import kusto_database as kdb
from pytest import raises
from .fake_database import FakeDatabase


def test_tableexpr_getattr():
    """Columns can be got from table with . operator"""
    db = kdb.KustoDatabase("test", "testdb")
    tbl = db.table("tbl", columns={"foo": str, "bar": int})
    assert tbl.foo.dtype == str
    assert tbl.bar.dtype == int


def test_tableexpr_getattr_bracket():
    """Columns can be got from table with [] operator"""
    db = kdb.KustoDatabase("test", "testdb")
    tbl = db.table("tbl", columns={"foo": str, "bar": int})
    assert tbl["foo"].dtype == str
    assert tbl["bar"].dtype == int


def test_tableexpr_getattr_column():
    """Creating table with list of Column works."""
    db = kdb.KustoDatabase("test", "testdb")
    tbl = db.table("tbl", columns=[kdb.Column("foo", str), kdb.Column("bar", int)])
    assert tbl.foo.dtype == str
    assert tbl.bar.dtype == int


def test_unknown_column_raises():
    """Accessing an unknown column errors."""
    db = kdb.KustoDatabase("test", "testdb")
    tbl = db.table("tbl", columns=[kdb.Column("foo", str), kdb.Column("bar", int)])
    with raises(AttributeError):
        return tbl.baz


def test_no_columns():
    """A table can be created with no columns, but accessing one errors."""
    db = kdb.KustoDatabase("test", "testdb")
    tbl = db.table("tbl")
    with raises(AttributeError):
        return tbl.baz


def test_columns_valueerror():
    """Passing something else as columns errors."""
    db = kdb.KustoDatabase("test", "testdb")
    with raises(ValueError):
        return db.table("tbl", columns=db)


def test_tableexpr_project():
    """Project statement generates correctly."""
    db = kdb.KustoDatabase("test", "testdb")
    tbl = db.table("tbl", columns={"foo": str, "bar": int})
    query = str(tbl.project(tbl.foo, tbl.bar, baz=tbl.bar))
    expected = "cluster('test').database('testdb').['tbl']\n| project foo,\nbar,\nbaz = bar\n"
    assert query == expected


def test_collect():
    db = FakeDatabase("test", "testdb")
    tbl = kdb.TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.project(tbl.foo, tbl.bar, baz=tbl.bar).collect()
    expected = "cluster('test').database('testdb').['tbl']\n| project foo,\nbar,\nbaz = bar\n"
    assert query == expected


def test_count():
    db = FakeDatabase("test", "testdb")
    tbl = kdb.TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = str(tbl.project(tbl.foo, tbl.bar).count())
    expected = "cluster('test').database('testdb').['tbl']\n| project foo,\nbar\n| count\n"
    assert query == expected


def test_count_repr():
    assert repr(kdb.Count()) == "Count()"


def test_table_distinct():
    db = FakeDatabase("test", "testdb")
    tbl = kdb.TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    q = str(tbl.distinct(tbl.bar, tbl.foo))
    ex = "cluster('test').database('testdb').['tbl']\n| distinct bar,\nfoo\n"
    assert q == ex


def test_distinct():
    query = str(kdb.Distinct(kdb.Column("bar", str), kdb.Column("foo", int)))
    expected = "| distinct bar,\nfoo"
    assert query == expected


def test_column_repr():
    assert repr(kdb.Distinct("foo", "bar")) == "Distinct(foo, bar)"


def test_expression_repr():
    assert repr(kdb.Column("foo", str)) == "Column(\"foo\", <class 'str'>)"
