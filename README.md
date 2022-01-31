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
- [ ] asc, desc
- [ ] datatable
- [ ] evaluate
- [ ] facet
- [ ] find
- [ ] fork
- [ ] getschema
- [ ] invoke
- [ ] lookup
- [ ] mv-apply
- [ ] mv-expand
- [ ] parse
- [ ] partition
- [ ] pivot
- [ ] range
- [ ] sample, sample-distinct
- [ ] search
- [ ] serialize
- [ ] take, limit
- [ ] top, top-hitters
- [ ] Kusto prefix function translator class
- [ ] special types (datetime, timespan, dynamic)

