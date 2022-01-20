from kusto_tool import kusto_database as kdb


def test_project():
    """Test project statement"""
    db = kdb.KustoDatabase("test", "testdb")
    tbl = db.table("tbl")
    query = str(tbl.project("foo", "bar", "baz"))
    expected = "['tbl']\n| project foo,\nbar,\nbaz\n"
    assert query == expected
