# Naming conventions

## General
- tables and column names use camel_case
- tables and column names are in english
- models follow the naming convention:
    <layer>_<source>__<entity>.sql
- we use plural for models

## bronze
## silver
## gold
- fact tables are prefixed with fct_ and dimensions with dim_ and aggregations with agg_
## technical columns
- will be prefixed with dwh_
