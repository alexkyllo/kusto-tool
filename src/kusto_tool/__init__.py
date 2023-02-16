"""A high-level API for querying Kusto."""
__version__ = "0.4.2"

from .database import Cluster, KustoDatabase, cluster
from .expression import TableExpr

__all__ = ["KustoDatabase", "Cluster", "cluster", "TableExpr"]
