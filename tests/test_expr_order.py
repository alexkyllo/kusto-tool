from kusto_tool.expression import Column, Order


def test_order_1arg():
    """order by works"""
    assert str(Order(Column("foo", str))) == "| order by\n\tfoo"


def test_order_2args():
    """order by works with 2 args"""
    assert str(Order(Column("foo", str), Column("bar", str))) == "| order by\n\tfoo,\n\tbar"


def test_order_asc():
    """order works with asc"""
    col = Column("foo", str).asc()
    expr = Order(col)
    assert str(expr) == "| order by\n\tfoo asc"


def test_order_asc_multiple():
    """order works with asc"""
    expr = Order(Column("foo", str).desc(), Column("bar", str).asc(), Column("baz", str).desc())
    assert str(expr) == "| order by\n\tfoo,\n\tbar asc,\n\tbaz"
