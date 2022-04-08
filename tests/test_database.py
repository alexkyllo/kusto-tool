from azure.kusto.data.helpers import dataframe_from_result_table
from pytest import raises

from kusto_tool import database as kdb
from kusto_tool import expression as exp

from .fake_database import FakeDatabase, FakeKustoClient, FakeKustoResultTable


def test_dict_to_datatable():
    dct = {"foo": "one", "bar": "two", "baz": "three"}
    expected = (
        "datatable(key: string, value: string)[\n"
        "    'foo', 'one',\n\t'bar', 'two',\n\t'baz', 'three',\n]"
    )
    assert kdb.dict_to_datatable(dct) == expected


def test_list_to_kusto():
    """A list of strings renders as a Kusto list."""
    lst = ["foo", "bar", "baz"]
    expected = "dynamic([\n\t'foo',\n\t'bar',\n\t'baz'\n])"
    assert kdb.list_to_kusto(lst) == expected


def test_list_to_kusto_int():
    """A list of ints renders as a Kusto list."""
    lst = [1, 2, 3]
    expected = "dynamic([\n\t1,\n\t2,\n\t3\n])"
    assert kdb.list_to_kusto(lst) == expected


def test_render_template_query():
    foo = "bar"
    actual = kdb.render_template_query("{{ foo }} | take 10", foo=foo)
    expected = "bar | take 10"
    assert actual == expected


def test_render_set():
    result = kdb.render_set(
        "StormEvents | take 10",
        table="StormEventsTake10",
        folder="myfolder",
        docstring="mydocstring",
    )
    expected = """.set-or-append StormEventsTake10
with (
folder = "myfolder",
docstring = "mydocstring",
)
<|
StormEvents | take 10"""
    assert result == expected


def test_render_set_replace():
    result = kdb.render_set(
        "StormEvents | take 10",
        table="StormEventsTake10",
        folder="myfolder",
        docstring="mydocstring",
        replace=True,
    )
    expected = """.set-or-replace StormEventsTake10
with (
folder = "myfolder",
docstring = "mydocstring",
)
<|
StormEvents | take 10"""
    assert result == expected


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
    tbl = db.table("tbl", columns=[exp.Column("foo", str), exp.Column("bar", int)])
    assert tbl.foo.dtype == str
    assert tbl.bar.dtype == int


def test_unknown_column_raises():
    """Accessing an unknown column errors."""
    db = kdb.KustoDatabase("test", "testdb")
    tbl = db.table("tbl", columns=[exp.Column("foo", str), exp.Column("bar", int)])
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
    expected = (
        "cluster('test').database('testdb').['tbl']\n| project\n\tfoo,\n\tbar,\n\tbaz = bar\n"
    )
    assert query == expected


def test_collect():
    db = FakeDatabase("test", "testdb")
    tbl = kdb.TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = tbl.project(tbl.foo, tbl.bar, baz=tbl.bar).collect()
    expected = (
        "cluster('test').database('testdb').['tbl']\n| project\n\tfoo,\n\tbar,\n\tbaz = bar\n"
    )
    assert query == expected


def test_count():
    db = FakeDatabase("test", "testdb")
    tbl = kdb.TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    query = str(tbl.project(tbl.foo, tbl.bar).count())
    expected = "cluster('test').database('testdb').['tbl']\n| project\n\tfoo,\n\tbar\n| count\n"
    assert query == expected


def test_count_repr():
    assert repr(exp.Count()) == "Count()"


def test_table_distinct():
    db = FakeDatabase("test", "testdb")
    tbl = kdb.TableExpr("tbl", database=db, columns={"foo": str, "bar": int})
    q = str(tbl.distinct(tbl.bar, tbl.foo))
    ex = "cluster('test').database('testdb').['tbl']\n| distinct bar,\nfoo\n"
    assert q == ex


def test_distinct():
    query = str(exp.Distinct(exp.Column("bar", str), exp.Column("foo", int)))
    expected = "| distinct bar,\nfoo"
    assert query == expected


def test_column_repr():
    assert repr(exp.Distinct("foo", "bar")) == "Distinct(foo, bar)"


def test_expression_repr():
    assert repr(exp.Column("foo", str)) == "Column(\"foo\", <class 'str'>)"


def test_get_columns():
    db = FakeDatabase("test", "testdb")
    tbl = kdb.TableExpr("tbl", database=db, columns={"foo": str})
    assert tbl.foo == tbl.columns["foo"]


def test_extend():
    db = FakeDatabase("test", "testdb")
    tbl = kdb.TableExpr("tbl", database=db, columns={"foo": str})
    tbl_2 = tbl.extend(bar="baz")
    query = str(tbl_2)
    expected = "cluster('test').database('testdb').['tbl']\n| extend\n\tbar='baz'\n"
    assert "bar" in tbl_2.columns
    assert "bar" not in tbl.columns
    assert query == expected


def test_summarize_adds_aliases():
    """summarize adds aliases to column list."""
    tbl = (
        kdb.cluster("help")
        .database("Samples")
        .table("StormEvents", columns={"State": str, "EventType": str, "DamageProperty": int})
    )
    query = tbl.project(tbl.State, tbl.EventType, tbl.DamageProperty).summarize(
        sum_damage=tbl.DamageProperty.sum(), by=[tbl.State, tbl.EventType]
    )
    query = query.sort(query.sum_damage).limit(20)
    expected = """cluster('help').database('Samples').['StormEvents']
| project
\tState,
\tEventType,
\tDamageProperty
| summarize
\tsum_damage=sum(DamageProperty)
\tby State, EventType
| order by
\tsum_damage
| limit 20
"""
    assert str(query) == expected


def test_long_query():
    """test a longer query"""
    tbl = (
        kdb.cluster("help")
        .database("Samples")
        .table("StormEvents", columns={"State": str, "EventType": str, "DamageProperty": int})
    )
    query = (
        tbl.project(tbl.State, tbl.EventType, tbl.DamageProperty)
        .summarize(sum_damage=tbl.DamageProperty.sum(), by=[tbl.State, tbl.EventType])
        .sort("sum_damage")
        .limit(20)
    )
    expected = """cluster('help').database('Samples').['StormEvents']
| project
\tState,
\tEventType,
\tDamageProperty
| summarize
\tsum_damage=sum(DamageProperty)
\tby State, EventType
| order by
\tsum_damage
| limit 20
"""
    assert str(query) == expected


def test_expr_idempotent():
    """Querying a tbl multiple times is idempotent."""
    tbl = (
        kdb.cluster("help")
        .database("Samples")
        .table("StormEvents", columns={"State": str, "EventType": str, "DamageProperty": int})
    )
    query = tbl.project(tbl.State, tbl.EventType, tbl.DamageProperty)
    query = tbl.project(tbl.State, tbl.EventType, tbl.DamageProperty)
    expected = """cluster('help').database('Samples').['StormEvents']
| project
\tState,
\tEventType,
\tDamageProperty
"""
    assert str(query) == expected
