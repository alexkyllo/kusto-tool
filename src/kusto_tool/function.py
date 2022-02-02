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
