from kusto_tool import kusto_database as kdb
from .fake_database import FakeDatabase
from pytest import fixture


@fixture
def db():
    return FakeDatabase("test", "testdb")


@fixture
def tbl(db):
    return kdb.TableExpr("tbl", database=db, columns={"foo": str, "bar": int})


def test_where_eq_str():
    actual = str(kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.EQ, "a")))
    expected = "| where foo == 'a'"
    assert actual == expected


def test_where_eq_int():
    actual = str(kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.EQ, 2)))
    expected = "| where foo == 2"
    assert actual == expected


def test_where_ne_int():
    actual = str(kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.NE, 2)))
    expected = "| where foo != 2"
    assert actual == expected


def test_where_lt_int():
    actual = str(kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.LT, 2)))
    expected = "| where foo < 2"
    assert actual == expected


def test_where_lte_int():
    actual = str(kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.LE, 2)))
    expected = "| where foo <= 2"
    assert actual == expected


def test_where_gt_int():
    actual = str(kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.GT, 2)))
    expected = "| where foo > 2"
    assert actual == expected


def test_where_gte_int():
    actual = str(kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.GE, 2)))
    expected = "| where foo >= 2"
    assert actual == expected


def test_table_where_eq(tbl):
    q = str(tbl.where(tbl.bar == "foo"))
    ex = "cluster('test').database('testdb').['tbl']\n| where bar == 'foo'\n"
    assert q == ex


def test_table_where_ne(tbl):
    q = str(tbl.where(tbl.bar != "foo"))
    ex = "cluster('test').database('testdb').['tbl']\n| where bar != 'foo'\n"
    assert q == ex


def test_table_where_lt(tbl):
    q = str(tbl.where(tbl.bar < "foo"))
    ex = "cluster('test').database('testdb').['tbl']\n| where bar < 'foo'\n"
    assert q == ex


def test_table_where_le(tbl):
    q = str(tbl.where(tbl.bar <= "foo"))
    ex = "cluster('test').database('testdb').['tbl']\n| where bar <= 'foo'\n"
    assert q == ex


def test_table_where_gt(tbl):
    q = str(tbl.where(tbl.bar > "foo"))
    ex = "cluster('test').database('testdb').['tbl']\n| where bar > 'foo'\n"
    assert q == ex


def test_table_where_ge(tbl):
    q = str(tbl.where(tbl.bar >= "foo"))
    ex = "cluster('test').database('testdb').['tbl']\n| where bar >= 'foo'\n"
    assert q == ex


def test_where_repr():
    where = kdb.Where(kdb.Expression(kdb.Column("foo", str), kdb.OP.EQ, 2))
    assert repr(where) == "Where(Column(\"foo\", <class 'str'>) == 2)"
