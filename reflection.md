1. load and anylyze the dataset
``` shell
> ./duckdb test.db
> .read ./data/schema.sql
> .read ./data/load.sql
> analyze;
```
2. Create a schema for Calcite
change the project to maven cuz gradle sucks

