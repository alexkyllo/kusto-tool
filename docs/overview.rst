Overview of `kusto_tool`
========================

`kusto-tool` is a Python package providing a high-level interface for using
`Azure Data Explorer`_, a log analytics database service from Microsoft.

The main feature of `kusto_tool` is a Python expression API for querying Kusto
directly from Python code, using method chaining to mimic Kusto Query Language
(KQL)'s pipe-based query structure.

.. code-block:: python

    query = (
        cluster("help")
        .database("Samples")
        .table("StormEvents")
        .project()
    )

 .. _Azure Data Explorer: https://azure.microsoft.com/en-us/services/data-explorer/