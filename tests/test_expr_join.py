from kusto_tool.expression import Join, TableExpr

from .fake_database import FakeDatabase


def test_join_str():
    t2 = TableExpr("table2", "db", columns=dict(foo=str, bar=str, baz=int))
    join = Join(t2, on=["foo", "bar"], kind="inner")
    expected = """| join kind=inner hint.strategy=shuffle (
    table2) on foo, bar"""
    assert str(join == expected)


def test_table_join():
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
