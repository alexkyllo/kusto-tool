from kusto_tool import kusto_database as kdb
from .fake_database import FakeDatabase


def test_where_eq_str():
    actual = str(kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.EQ, "a")))
    expected = "| where foo == 'a'"
    assert actual == expected


def test_where_eq_int():
    actual = str(kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.EQ, 2)))
    expected = "| where foo == 2"
    assert actual == expected


def test_table_where():
    db = FakeDatabase("test", "testdb")
    tbl = kdb.TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    q = str(tbl.where(tbl.bar == "foo"))
    ex = "cluster('test').database('testdb').['tbl']\n| where bar == 'foo'\n"
    assert q == ex


def test_where_repr():
    where = kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.EQ, 2))
    assert repr(where) == "Where(Column(\"foo\", <class 'str'>) == 2)"
