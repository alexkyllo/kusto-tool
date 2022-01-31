from kusto_tool.expression import UnaryExpression


def strcat(*args):
    """String concatenation.

    Parameters
    ----------
    args: list
        List of string Columns and/or scalar strings to concatenate.
    """
    return UnaryExpression("strcat", *args)
