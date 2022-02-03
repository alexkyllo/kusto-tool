from kusto_tool.expression import OP, Column, Extend, Infix, Prefix, typeof


def test_extend():
    """extend renders"""
    assert str(Extend(foo="bar")) == "| extend\n\tfoo='bar'"


def test_extend_two():
    """extend renders with two kwargs"""
    assert str(Extend(foo="bar", baz="quux")) == "| extend\n\tfoo='bar',\n\tbaz='quux'"


def test_typeof_bin():
    """type of binary expression works"""
    assert typeof(Infix(OP.ADD, Column("foo", int), 1)) == int


def test_typeof_unary():
    """type of unary expression works"""
    assert typeof(Prefix(OP.NOT, Column("foo", int))) == int


def test_typeof():
    """type of eagerly evaluated Python expression works"""
    assert typeof("foo" + "bar") == str
