from datetime import datetime
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


def startofday(expr, offset=None):
    """Get the start of day for a timestamp (round to preceding midnight).

    Parameters
    ----------
    expr: str, Column or expression.
        The datetime column/expression to round to the start of the day.
    offset: int, default None
        The number of days to shift the date by (-1 subtracts a day, 1 adds a day.)
    """
    if offset:
        return Prefix("startofday", expr, offset, agg=False, dtype=datetime)
    return Prefix("startofday", expr, agg=False, dtype=datetime)


def endofday(expr, offset=None):
    """Get the end of day for a timestamp (round to 11:59:59.9999999).

    Parameters
    ----------
    expr: str, Column or expression.
        The datetime column/expression to round to the end of the day.
    offset: int, default None
        The number of days to shift the date by (-1 subtracts a day, 1 adds a day.)
    """
    if offset:
        return Prefix("endofday", expr, offset, agg=False, dtype=datetime)
    return Prefix("endofday", expr, agg=False, dtype=datetime)


def startofweek(expr, offset=None):
    """Get the start of the week for a timestamp (round to preceding Sunday at midnight).

    Parameters
    ----------
    expr: str, Column or expression.
        The datetime column/expression to round to the start of the week.
    offset: int, default None
        The number of weeks to shift the date by (-1 subtracts a week, 1 adds a week.)
    """
    if offset:
        return Prefix("startofweek", expr, offset, agg=False, dtype=datetime)
    return Prefix("startofweek", expr, agg=False, dtype=datetime)


def endofweek(expr, offset=None):
    """Get the end of the week for a timestamp (round to following Saturday at 11:59:59.9999999).

    Parameters
    ----------
    expr: str, Column or expression.
        The datetime column/expression to round to the start of the week.
    offset: int, default None
        The number of weeks to shift the date by (-1 subtracts a week, 1 adds a week.)
    """
    if offset:
        return Prefix("endofweek", expr, offset, agg=False, dtype=datetime)
    return Prefix("endofweek", expr, agg=False, dtype=datetime)


def startofmonth(expr, offset=None):
    """Get the start of the month for a timestamp (round to first of the month).

    Parameters
    ----------
    expr: str, Column or expression.
        The datetime column/expression to round to the start of the month.
    offset: int, default None
        The number of months to shift the date by (-1 subtracts a month, 1 adds a month.)
    """
    if offset:
        return Prefix("startofmonth", expr, offset, agg=False, dtype=datetime)
    return Prefix("startofmonth", expr, agg=False, dtype=datetime)


def endofmonth(expr, offset=None):
    """Get the end of the month for a timestamp (round to last day of month at 11:59:59.9999999).

    Parameters
    ----------
    expr: str, Column or expression.
        The datetime column/expression to round to the start of the month.
    offset: int, default None
        The number of months to shift the date by (-1 subtracts a month, 1 adds a month.)
    """
    if offset:
        return Prefix("endofmonth", expr, offset, agg=False, dtype=datetime)
    return Prefix("endofmonth", expr, agg=False, dtype=datetime)


def startofyear(expr, offset=None):
    """Get the start of the year for a timestamp (round to first of the year).

    Parameters
    ----------
    expr: str, Column or expression.
        The datetime column/expression to round to the start of the year.
    offset: int, default None
        The number of years to shift the date by (-1 subtracts a year, 1 adds a year.)
    """
    if offset:
        return Prefix("startofyear", expr, offset, agg=False, dtype=datetime)
    return Prefix("startofyear", expr, agg=False, dtype=datetime)


def endofyear(expr, offset=None):
    """Get the end of the year for a timestamp (round to last day of year at 11:59:59.9999999).

    Parameters
    ----------
    expr: str, Column or expression.
        The datetime column/expression to round to the start of the year.
    offset: int, default None
        The number of years to shift the date by (-1 subtracts a year, 1 adds a year.)
    """
    if offset:
        return Prefix("endofyear", expr, offset, agg=False, dtype=datetime)
    return Prefix("endofyear", expr, agg=False, dtype=datetime)
