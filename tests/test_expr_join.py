from kusto_tool.expression import Join, TableExpr

from .fake_database import FakeDatabase


def test_join_str():
    t2 = TableExpr("table2", "db", columns=dict(foo=str, bar=str, baz=int))
    join = Join(t2, on=["foo", "bar"], kind="inner")
    expected = """| join kind=inner hint.strategy=shuffle (
    table2) on foo, bar"""
    assert str(join == expected)


def test_table_shuffle_join():
    db = FakeDatabase("test", "testdb")
    t1 = TableExpr("table1", db, columns=dict(foo=str, bar=str, baz=int))
    t2 = TableExpr("table2", db, columns=dict(foo=str, bar=str, baz=int))
    join = t1.join(t2, on=["foo", "bar"], kind="inner", strategy="shuffle")
    expected = """cluster('test').database('testdb').['table1']
| join kind=inner hint.strategy=shuffle (
\tcluster('test').database('testdb').['table2']
) on foo, bar
"""
    print(expected)
    print(join)
    assert str(join) == expected


def test_table_shuffle_join():
    db = FakeDatabase("test", "testdb")
    t1 = TableExpr("table1", db, columns=dict(foo=str, bar=str, baz=int))
    t2 = TableExpr("table2", db, columns=dict(foo=str, bar=str, baz=int))
    join = t1.join(t2, on=["foo", "bar"], kind="inner")
    expected = """cluster('test').database('testdb').['table1']
| join kind=inner (
\tcluster('test').database('testdb').['table2']
) on foo, bar
"""
    print(expected)
    print(join)
    assert str(join) == expected


def test_broadcast_join_str():
    db = FakeDatabase("test", "testdb")
    t1 = TableExpr("table1", db, columns=dict(foo=str, bar=str, baz=int))
    result = str(Join(t1, on="foo", kind="inner", strategy="broadcast"))
    expected = """| join kind=inner hint.strategy=broadcast (
\tcluster('test').database('testdb').['table1']
) on foo"""
    assert result == expected


def test_other_join_str():
    db = FakeDatabase("test", "testdb")
    t1 = TableExpr("table1", db, columns=dict(foo=str, bar=str, baz=int))
    result = str(Join(t1, on="foo", kind="inner", strategy="silly"))
    expected = """| join kind=inner (
\tcluster('test').database('testdb').['table1']
) on foo"""
    assert result == expected
