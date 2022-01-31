from kusto_tool import expression as exp
from pytest import fixture

from .fake_database import FakeDatabase


@fixture
def db():
    return FakeDatabase("test", "testdb")


@fixture
def tbl(db):
    return exp.TableExpr("tbl", database=db, columns={"foo": str, "bar": int})


def test_where_eq_str():
    actual = str(exp.Where(exp.BinaryExpression(exp.Column("foo", str), exp.OP.EQ, "a")))
    expected = "| where foo == 'a'"
    assert actual == expected


def test_where_eq_int():
    actual = str(exp.Where(exp.BinaryExpression(exp.Column("foo", str), exp.OP.EQ, 2)))
    expected = "| where foo == 2"
    assert actual == expected


def test_where_ne_int():
    actual = str(exp.Where(exp.BinaryExpression(exp.Column("foo", str), exp.OP.NE, 2)))
    expected = "| where foo != 2"
    assert actual == expected


def test_where_lt_int():
    actual = str(exp.Where(exp.BinaryExpression(exp.Column("foo", str), exp.OP.LT, 2)))
    expected = "| where foo < 2"
    assert actual == expected


def test_where_lte_int():
    actual = str(exp.Where(exp.BinaryExpression(exp.Column("foo", str), exp.OP.LE, 2)))
    expected = "| where foo <= 2"
    assert actual == expected


def test_where_gt_int():
    actual = str(exp.Where(exp.BinaryExpression(exp.Column("foo", str), exp.OP.GT, 2)))
    expected = "| where foo > 2"
    assert actual == expected


def test_where_gte_int():
    actual = str(exp.Where(exp.BinaryExpression(exp.Column("foo", str), exp.OP.GE, 2)))
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
    where = exp.Where(exp.BinaryExpression(exp.Column("foo", str), exp.OP.EQ, 2))
    assert repr(where) == "Where(Column(\"foo\", <class 'str'>) == 2)"


def test_where_and():
    foo = exp.Column("foo", str)
    bar = exp.Column("bar", str)
    where = exp.Where((foo == "a") & (bar == "b"))
    assert str(where) == "| where foo == 'a' and bar == 'b'"


def test_where_or():
    foo = exp.Column("foo", str)
    bar = exp.Column("bar", str)
    where = exp.Where((foo == "a") | (bar == "b"))
    assert str(where) == "| where foo == 'a' or bar == 'b'"
