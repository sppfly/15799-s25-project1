1. load and anylyze the dataset
``` shell
> ./duckdb test.db
> .read ./data/schema.sql
> .read ./data/load.sql
> analyze;
```
2. Create a schema for Calcite
change the project to maven cuz gradle sucks

At first I tried to create a JDBC connection to JDBC, and wrap it to calcite, but it does not work

The right way is :
- create a connection to calcite (I dont know why) and get a rootSchema
- get basicDatasource for duckdb
- create schema with `JdbcSchema.create`
- add this shcema to the rootSchema



3. Get Calcite to parse SQL queries and print them to stdout.

This part is really (麻烦) when u do not understand how Calcite works


Basically it works like this:

``` Java
var parser = SqlParser.create(sql);

var sqlNode = parser.parseQuery();

var relRoot = sqlToRelConverter.convertQuery(sqlNode, 
                        true, // whether to validator the query
                        true // whether the node the the top node
                        );
```
The point of this step is HOW to get this sqlToRelConverter


PROBLEM: somehow q1 does not pass the validation, I guess it's because of the difference of different SQL standard, maybe I need to set it to DuckDB/PostgresSQL in validator, not sure though.

4. Enable the heuristic optimizer with a few rules, test that it works.


The problem is that I do not know what rules should I add, QUESTION: how to know?

I asked the mailing list of Apache Calcite, it turned out the choice of rules is like art instead of science, so basically I need to be famililar with the queries.

As a result I'll just add the rules of textbooks, like no cartisien product, push filter and projection down, etc.

5. Experiment with the rules.
I'm going to ask ChatGPT what rules should I add, according from the sql

or I'll just add all rules, using reflection in Java
6. Implement support for going back to SQL.
DONE
7. Support RelRunner execution.
I'm not doing it right now

8. Enable the cost-based query optimizer.
9. Print the cardinality of a table. If you get 100, you may be using the hardcoded default.
10. Implement statistics (at least support actual table cardinalities).
11. Experiment with the rules more, implement better statistics, etc.