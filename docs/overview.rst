Overview of `kusto_tool`
========================

`kusto-tool` is a Python package providing a high-level interface for using
`Azure Data Explorer`_, a log analytics database service from Microsoft.

The main feature of `kusto_tool` is a Python expression API for generating and
running Kusto queries directly from Python code, using method chaining to mimic
Kusto Query Language (KQL)'s pipe-based query structure.

.. code-block:: python

    tbl = (
        kdb.cluster("help")
        .database("Samples")
        .table("StormEvents", inspect=True) # queries the database for column list
    )
    query = (
        tbl.project(tbl.State, tbl.EventType, tbl.DamageProperty)
        .summarize(sum_damage=tbl.DamageProperty.sum(), by=[tbl.State, tbl.EventType])
        .sort(tbl.sum_damage)
        .limit(20)
    )
    print(query)

    # cluster('help').database('Samples').['StormEvents']
    # | project
    #     State,
    #     EventType,
    #     DamageProperty
    # | summarize
    #     sum_damage=sum(DamageProperty)
    #     by State, EventType
    # | order by
    #     sum_damage
    # | limit 20


.. _Azure Data Explorer: https://azure.microsoft.com/en-us/services/data-explorer/