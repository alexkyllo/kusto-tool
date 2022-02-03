![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)

# kusto-tool

A high-level Python library and CLI to make it easier to use Azure Data Explorer
(aka Kusto).

Experimental, work-in-progress, unstable API.

## TODO

### Database API

#### Table management 

- [x] .set-or-append table
- [x] .set-or-replace table
- [x] .drop table
- [ ] .append table
- [ ] .create table
- [ ] .create-merge table
 
#### Function management

- [ ] .create-or-alter function
- [ ] .drop function

#### Policy management

- [ ] retention policy
- [ ] sharding policy

#### Access management

- [ ] .show [table|database] principals
- [ ] .add [table|database] [users|admins]

### Query expression API

- [x] project
- [x] where
- [x] distinct
- [x] summarize
- [x] join
- [x] parenthesize or/and expressions
- [x] extend
- [x] order by, sort
- [x] asc, desc
- [x] evaluate
- [x] take, limit
- [x] mv-expand
- [x] Inspect columns from table by querying
- [ ] mv-apply
- [ ] datatable
- [ ] facet
- [ ] find
- [ ] fork
- [ ] getschema
- [ ] invoke
- [ ] lookup
- [ ] parse
- [ ] partition
- [ ] pivot
- [ ] range
- [ ] sample, sample-distinct
- [ ] search
- [ ] serialize
- [ ] top, top-hitters
- [ ] Kusto prefix function translator class
- [ ] special types (datetime, timespan, dynamic)
- [ ] nice error messages when column not found in table etc.

### Agg functions

- [ ] sumif
- [ ] countif
- [ ] dcountif
- [ ] binary_all_and
- [ ] binary_all_or
- [ ] binary_all_xor
- [ ] make_bag / make_set / make_list
- [ ] arg_max, arg_min, take_any, take_anyif
- [ ] avgif
- [ ] hll, hll_merge
- [ ] max, maxif, min, minif
- [ ] percentiles, percentiles_array, percentilesw
- [ ] stdev, stdevif
- [ ] tdigest, tdigest_merge
- [ ] variance, variancep, varianceif