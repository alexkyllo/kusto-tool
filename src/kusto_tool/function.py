from datetime import datetime as dt
from datetime import timedelta as td
from decimal import Decimal
from typing import Any

from kusto_tool.expression import OP, Column, Prefix, typeof


def function(name, *args):
    """Translate to any Kusto function call.

    Parameters
    ----------
    name: str
        The name of the Kusto function to call.
    args: list
        A list of positional arguments to the function.
    """
    if len(args) > 0:
        Prefix(name, *args, dtype=typeof(args[0]))
    return Prefix(name, *args)


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
        return Prefix("startofday", expr, offset, agg=False, dtype=dt)
    return Prefix("startofday", expr, agg=False, dtype=dt)


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
        return Prefix("endofday", expr, offset, agg=False, dtype=dt)
    return Prefix("endofday", expr, agg=False, dtype=dt)


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
        return Prefix("startofweek", expr, offset, agg=False, dtype=dt)
    return Prefix("startofweek", expr, agg=False, dtype=dt)


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
        return Prefix("endofweek", expr, offset, agg=False, dtype=dt)
    return Prefix("endofweek", expr, agg=False, dtype=dt)


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
        return Prefix("startofmonth", expr, offset, agg=False, dtype=dt)
    return Prefix("startofmonth", expr, agg=False, dtype=dt)


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
        return Prefix("endofmonth", expr, offset, agg=False, dtype=dt)
    return Prefix("endofmonth", expr, agg=False, dtype=dt)


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
        return Prefix("startofyear", expr, offset, agg=False, dtype=dt)
    return Prefix("startofyear", expr, agg=False, dtype=dt)


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
        return Prefix("endofyear", expr, offset, agg=False, dtype=dt)
    return Prefix("endofyear", expr, agg=False, dtype=dt)


def tobool(expr):
    """Convert an expression to boolean."""
    return Prefix("tobool", expr, agg=False, dtype=bool)


def datetime(expr):
    """Construct a datetime literal."""
    return Prefix("datetime", expr, agg=False, dtype=dt)


def todatetime(expr):
    """Convert an expression to a datetime."""
    return Prefix("todatetime", expr, agg=False, dtype=dt)


def int_(expr):
    """Construct an int from a string literal."""
    return Prefix("int", expr, agg=False, dtype=int)


def toint(expr):
    """Converts input to an integer."""
    return Prefix("toint", expr, agg=False, dtype=int)


def long_(expr):
    """Construct a long from a string literal."""
    return Prefix("long", expr, agg=False, dtype=int)


def tolong(expr):
    """Convert an expression to long (signed 64 bit integer)."""
    return Prefix("tolong", expr, agg=False, dtype=int)


def decimal(expr):
    """Construct a decimal from a literal."""
    return Prefix("decimal", expr, agg=False, dtype=Decimal)


def todecimal(expr):
    """Convert an expression to decimal (fixed point)."""
    return Prefix("todecimal", expr, agg=False, dtype=Decimal)


def todouble(expr):
    """Convert an expression to double (signed 64 bit floating point)."""
    return Prefix("todouble", expr, agg=False, dtype=float)


def toreal(expr):
    """Convert an expression to real (signed 64 bit floating point)."""
    return Prefix("toreal", expr, agg=False, dtype=float)


def todynamic(expr):
    """Parse a JSON string to a dynamic."""
    return Prefix("todynamic", expr, agg=False, dtype=object)


def parse_json(expr):
    """Parse a JSON string to a dynamic."""
    return Prefix("parse_json", expr, agg=False, dtype=object)


def tohex(expr):
    """Converts input to a hexadecimal string."""
    return Prefix("tohex", expr, agg=False, dtype=str)


def tostring(expr):
    """Converts input to a string."""
    return Prefix("tostring", expr, agg=False, dtype=str)


def totimespan(expr):
    """Converts input to a timespan."""
    return Prefix("totimespan", expr, agg=False, dtype=td)


def time(expr):
    """Construct a timespan from a string literal."""
    return Prefix("time", expr, agg=False, dtype=td)


def ago(expr):
    """Subtract a timespan from the current time."""
    return Prefix("ago", expr, agg=False, dtype=dt)


# Math


def abs_(expr):
    """Absolute value."""
    return Prefix("abs", expr, agg=False, dtype=typeof(expr))


def sin(expr):
    """Sine function."""
    return Prefix("sin", expr, agg=False, dtype=float)


def asin(expr):
    """Arcsine function."""
    return Prefix("asin", expr, agg=False, dtype=float)


def cos(expr):
    """Cosine function."""
    return Prefix("cos", expr, agg=False, dtype=float)


def acos(expr):
    """Arccosine function."""
    return Prefix("acos", expr, agg=False, dtype=float)


def tan(expr):
    """Tangent function."""
    return Prefix("tan", expr, agg=False, dtype=float)


def atan(expr):
    """Arctangent function."""
    return Prefix("atan", expr, agg=False, dtype=float)


def cot(expr):
    """Cotangent function."""
    return Prefix("cot", expr, agg=False, dtype=float)


def exp(expr):
    """Exponentiation function e^x."""
    return Prefix("exp", expr, agg=False, dtype=float)


def exp2(expr):
    """Exponentiation function 2^x."""
    return Prefix("exp2", expr, agg=False, dtype=typeof(expr))


def exp10(expr):
    """Exponentiation function 10^x."""
    return Prefix("exp10", expr, agg=False, dtype=typeof(expr))


def floor(expr, round_to):
    """Rounds values down to an integer multiple of a given bin size."""
    return Prefix("floor", expr, round_to, agg=False, dtype=int)


def bin_(expr, round_to):
    """Rounds values down to an integer multiple of a given bin size."""
    return Prefix("bin", expr, round_to, agg=False, dtype=int)


def ceiling(expr):
    """Rounds up to the smallest integer greater than or equal to expr."""
    return Prefix("ceiling", expr, agg=False, dtype=int)


def gamma(expr):
    """Gamma function."""
    return Prefix("gamma", expr, agg=False, dtype=float)


def isinf(expr):
    """Returns True if expr is infinity (positive or negative)."""
    return Prefix("isinf", expr, agg=False, dtype=bool)


def isfinite(expr):
    """Returns False if expr is infinity (positive or negative)."""
    return Prefix("isfinite", expr, agg=False, dtype=bool)


def isnan(expr):
    """Returns True if expr is NaN (not a number)."""
    return Prefix("isnan", expr, agg=False, dtype=bool)


def jaccard_index(expr1, expr2):
    """Returns the Jaccard index (intersection over union) of two sets."""
    return Prefix("jaccard_index", expr1, expr2, agg=False, dtype=float)


def log(expr):
    """Returns the natural (base-e) logarithm of expr."""
    return Prefix("log", expr, agg=False, dtype=float)


def log2(expr):
    """Returns the binary (base-2) logarithm of expr."""
    return Prefix("log2", expr, agg=False, dtype=float)


def log10(expr):
    """Returns the decimal (base-10) logarithm of expr."""
    return Prefix("log10", expr, agg=False, dtype=float)


def loggamma(expr):
    """Returns the logarithm of the absolute value of the gamma function."""
    return Prefix("loggamma", expr, agg=False, dtype=float)


def sqrt(expr):
    """Square root function."""
    return Prefix("sqrt", expr, agg=False, dtype=float)


def sign(expr):
    """Returns -1 if expr is negative, 0 if zero, or 1 if positive."""
    return Prefix("sign", expr, agg=False, dtype=int)


def pi():
    """Returns the constant pi."""
    return Prefix("pi", agg=False, dtype=float)


def pow_(base, exponent):
    """Raises base to the exponent."""
    return Prefix("pow", base, exponent, agg=False, dtype=float)


def rand(maximum=1.0):
    """Return a random number between 0 and maximum (default 1.0)

    Parameters
    ----------
    maximum: float, default=1.0
        The maximum value for the randomly generated number.
    """
    return Prefix("rand", maximum, agg=False, dtype=float)
