from typing import Any

from kusto_tool.expression import OP, Column, Prefix


def strcat(*args):
    """String concatenation.

    Parameters
    ----------
    args: list
        List of string Columns and/or scalar strings to concatenate.
    """
    return Prefix(OP.STRCAT, *args, dtype=str)


def sum(expr):
    """Sum a column or expression.

    Parameters
    ----------
    expr: str, Column or expression.
    """
    # if sum gets a string, it's referring to a Column in the TableExpr.
    if isinstance(expr, str):
        expr = Column(expr, float)
    return Prefix(OP.SUM, expr, agg=True, dtype=float)


def avg(expr):
    """Average a column or expression.

    Parameters
    ----------
    expr: str, Column or expression.
    """
    if isinstance(expr, str):
        expr = Column(expr, float)
    return Prefix(OP.AVG, expr, agg=True, dtype=float)


def mean(expr):
    """Average a column or expression.

    Parameters
    ----------
    expr: str, Column or expression.
    """
    return avg(expr)


def count():
    """Count rows in the result set."""
    return Prefix(OP.COUNT, agg=True, dtype=int)


def dcount(expr, accuracy=1):
    """Distinct count of a column.

    Parameters
    ----------
    expr: str, Column or expression.
        The column to apply distinct count to.
    accuracy: int, default 1
        The level of accuracy to apply to the hyper log log algorithm.
        Default is 1, the fastest but least accurate.
    """
    if isinstance(expr, str):
        expr = Column(expr, Any)
    return Prefix(OP.DCOUNT, expr, accuracy, agg=True, dtype=int)
