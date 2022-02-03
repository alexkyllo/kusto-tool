from kusto_tool.expression import OP, Column, Extend, Infix, Prefix, typeof


def test_extend():
    """extend renders"""
    assert str(Extend(foo="bar")) == "| extend\n\tfoo='bar'"


def test_extend_two():
    """extend renders with two kwargs"""
    assert str(Extend(foo="bar", baz="quux")) == "| extend\n\tfoo='bar',\n\tbaz='quux'"


def test_typeof_bin():
    """type of binary expression works"""
    assert typeof(Column("foo", int) + 1) == int


def test_typeof_unary():
    """type of unary expression works"""
    assert typeof(~Column("foo", int)) == bool


def test_typeof():
    """type of eagerly evaluated Python expression works"""
    assert typeof("foo" + "bar") == str


def test_typeof_add_int_int():
    assert typeof(Column("foo", int) + Column("bar", int)) == int


def test_typeof_add_int_float():
    assert typeof(Column("foo", int) + Column("bar", float)) == float


def test_typeof_add_float_int():
    assert typeof(Column("foo", float) + Column("bar", int)) == float


def test_typeof_sub_float_int():
    assert typeof(Column("foo", float) - Column("bar", int)) == float


def test_typeof_sub_int_int():
    assert typeof(Column("foo", int) - Column("bar", int)) == int


def test_typeof_div_float_int():
    """type of eagerly evaluated Python expression works"""
    assert typeof(Column("foo", float) / Column("bar", int)) == float


def test_typeof_mul_float_int():
    """type of eagerly evaluated Python expression works"""
    assert typeof(Column("foo", float) * Column("bar", int)) == float


def test_typeof_mul_int_int():
    """type of eagerly evaluated Python expression works"""
    assert typeof(Column("foo", int) * Column("bar", int)) == int
