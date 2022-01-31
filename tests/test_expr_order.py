from kusto_tool.expression import Column, Order


def test_order_1arg():
    """order by works"""
    assert str(Order(Column("foo", str))) == "| order by\n\tfoo"


def test_order_2args():
    """order by works with 2 args"""
    assert str(Order(Column("foo", str), Column("bar", str))) == "| order by\n\tfoo,\n\tbar"


def test_order_asc():
    """order works with asc"""
    # assert str(Order(Column("foo", str)))
