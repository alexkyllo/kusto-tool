from typing import Any

from kusto_tool.expression import OP, Column, UnaryExpression


def strcat(*args):
    """String concatenation.

    Parameters
    ----------
    args: list
        List of string Columns and/or scalar strings to concatenate.
    """
    return UnaryExpression(OP.STRCAT, *args)


def sum(expr):
    """Sum a column or expression.

    Parameters
    ----------
    expr: str, Column or expression.
    """
    # if sum gets a string, it's referring to a Column in the TableExpr.
    if isinstance(expr, str):
        expr = Column(expr, Any)
    return UnaryExpression(OP.SUM, expr, agg=True)


def avg(expr):
    """Average a column or expression.

    Parameters
    ----------
    expr: str, Column or expression.
    """
    if isinstance(expr, str):
        expr = Column(expr, Any)
    return UnaryExpression(OP.AVG, expr, agg=True)


def mean(expr):
    """Average a column or expression.

    Parameters
    ----------
    expr: str, Column or expression.
    """
    return avg(expr)


def count():
    """Count rows in the result set."""
    return UnaryExpression(OP.COUNT, agg=True)


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
    return UnaryExpression(OP.DCOUNT, expr, accuracy, agg=True)
