Overview of `kusto-tool`
========================

`kusto-tool` is a Python package providing a high-level interface for using
`Azure Data Explorer`_, a log analytics database service from Microsoft.

This package is an experimental work-in-progress and the API is unstable.

Installation
------------

:code:`pip install kusto-tool`

The main feature of `kusto-tool` is a Python expression API for generating and
running Kusto queries directly from Python code, using method chaining to mimic
Kusto Query Language (KQL)'s pipe-based query structure.

.. code-block:: python

    from kusto_tool import cluster

    # If inspect=True, the database is queried to get the column schema.
    tbl = cluster("help").database("Samples").table("StormEvents", inspect=True)
    
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

It also provides a `KustoDatabase` class that helps with running queries.

.. code-block:: python

    from kusto_tool import KustoDatabase
    samples = KustoDatabase("help", "Samples")
    samples.execute("StormEvents | getschema")

    # 2022-02-16 08:33:23.562 | INFO     | kusto_tool.database:execute:150 - Executing query on Samples: StormEvents | getschema
    # 2022-02-16 08:33:26.826 | INFO     | kusto_tool.database:execute:155 - Query execution completed in 3.26 seconds.
    #         ColumnName  ColumnOrdinal         DataType ColumnType
    # 0          StartTime              0  System.DateTime   datetime
    # 1            EndTime              1  System.DateTime   datetime
    # 2          EpisodeId              2     System.Int32        int
    # 3            EventId              3     System.Int32        int
    # 4              State              4    System.String     string
    # 5          EventType              5    System.String     string
    # 6     InjuriesDirect              6     System.Int32        int
    # 7   InjuriesIndirect              7     System.Int32        int
    # 8       DeathsDirect              8     System.Int32        int
    # 9     DeathsIndirect              9     System.Int32        int
    # 10    DamageProperty             10     System.Int32        int
    # 11       DamageCrops             11     System.Int32        int
    # 12            Source             12    System.String     string
    # 13     BeginLocation             13    System.String     string
    # 14       EndLocation             14    System.String     string
    # 15          BeginLat             15    System.Double       real
    # 16          BeginLon             16    System.Double       real
    # 17            EndLat             17    System.Double       real
    # 18            EndLon             18    System.Double       real
    # 19  EpisodeNarrative             19    System.String     string
    # 20    EventNarrative             20    System.String     string
    # 21      StormSummary             21    System.Object    dynamic


If you don't provide a KustoClient to the :code:`client` parameter of :code:`KustoDatabase`,
it will default to Azure CLI authentication. 

:code:`pip install kusto-tool[azure-cli]`
will install the Azure CLI for you as an optional dependency, or you can install
it from `Microsoft Docs - How to install the Azure CLI`_.

Then run :code:`az login` to authenticate, and `kusto-tool` will use your cached
credentials to authenticate to Kusto.

.. _Azure Data Explorer: https://azure.microsoft.com/en-us/services/data-explorer/
.. _Microsoft Docs - How to install the Azure CLI: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli