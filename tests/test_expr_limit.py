from kusto_tool.database import KustoDatabase
from kusto_tool.expression import Limit, TableExpr


def test_limit():
    """limit prints."""
    assert str(Limit(1000)) == "| limit 1000"


def test_limit_tbl():
    """limit works on a tbl."""
    assert (
        str(TableExpr("tbl", KustoDatabase("c", "db")).limit(1000))
        == "cluster('c').database('db').['tbl']\n| limit 1000\n"
    )


def test_take():
    """take works."""
    assert (
        str(TableExpr("tbl", KustoDatabase("c", "db")).take(1000))
        == "cluster('c').database('db').['tbl']\n| limit 1000\n"
    )


def test_sample():
    """sample works."""
    assert (
        str(TableExpr("tbl", KustoDatabase("c", "db")).sample(1000))
        == "cluster('c').database('db').['tbl']\n| sample 1000\n"
    )


def test_sample_distinct():
    """sample-distinct works."""
    assert (
        str(
            TableExpr("tbl", KustoDatabase("c", "db"), columns=dict(foo=str)).sample_distinct(
                1000, "foo"
            )
        )
        == "cluster('c').database('db').['tbl']\n| sample 1000 of foo\n"
    )
