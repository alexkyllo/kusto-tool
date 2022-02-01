from kusto_tool.expression import OP, BinaryExpression, Column, Extend, UnaryExpression, typeof


def test_extend():
    """extend renders"""
    assert str(Extend(foo="bar")) == "| extend\n\tfoo='bar'"


def test_extend_two():
    """extend renders with two kwargs"""
    assert str(Extend(foo="bar", baz="quux")) == "| extend\n\tfoo='bar',\n\tbaz='quux'"


def test_typeof_bin():
    """type of binary expression works"""
    assert typeof(BinaryExpression(OP.ADD, Column("foo", int), 1)) == int


def test_typeof_unary():
    """type of unary expression works"""
    assert typeof(UnaryExpression(OP.NOT, Column("foo", int))) == int


def test_typeof():
    """type of eagerly evaluated Python expression works"""
    assert typeof("foo" + "bar") == str
