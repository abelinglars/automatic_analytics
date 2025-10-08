# Data Architecture

- we will use the medallion approach with 3 main layers of transformations in the warehouse
- conduct truncate and full loads for all layers

## bronze
- loading data as is
    - traceability and debugging

    - view
## silver
- cleaning and standardization
- renaming cols
- data enrichment
- derived columns
- !no joins
    - preparation for analysis
    - standardisation
    - single source of truth
    
    - view

## gold
- create star schema
- focus on user friendliness
- focus on business processes
- apply aggregates and business rules
- combining data
    - prepare for consumation

    - tables
