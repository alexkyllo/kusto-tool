import kusto_tool.function as F
from kusto_tool.expression import Column


def test_strcat():
    """strcat translates correctly"""
    assert str(F.strcat(Column("foo", str), "_", "bar")) == "strcat(foo, '_', 'bar')"
