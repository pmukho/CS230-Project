# Probabalistic Grammars and Query Structures

## Using Available Spark SQL Grammar
We use the [grammar files](https://github.com/apache/spark/tree/master/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser) found from the public Spark [repo](https://github.com/apache/spark).

Assume that your cwd looks something like this:
```
.
├── README.md (this file)
├── SqlBaseLexer.g4
└── SqlBaseParser.g4
```

## Building ANTLR4 Parser
1. Follow antlr4 [install guide](https://github.com/antlr/antlr4/blob/master/doc/getting-started.md#installation)
2. Run `antlr4 -o ./src/org/example -package org.example SqlBaseLexer.g4 SqlBaseParser.g4`
3. Verify that directory contents match the following:
```
.
├── README.md (this file)
├── SqlBaseLexer.g4
├── SqlBaseParser.g4
└── src
    ├── CountSparkQLParse.java
    └── org
        └── example
            ├── SqlBaseLexer.interp
            ├── SqlBaseLexer.java
            ├── SqlBaseLexer.tokens
            ├── SqlBaseParser.interp
            ├── SqlBaseParser.java
            ├── SqlBaseParser.tokens
            ├── SqlBaseParserBaseListener.java
            └── SqlBaseParserListener.java
```

## Build Counter Tool
```
$mkdir -p build
$javac -d build -cp /usr/local/lib/antlr-4.13.2-complete.jar src/org/example/*.java src/CountSparkQLRules.java
$java -cp build:/usr/local/lib/antlr-4.13.2-complete.jar CountSparkQLRules <your input file/directory>
```

## Output Format
Here is an example of the output produced.
```
Files processed: 1
Files with errors: 0
Rule counts:
StatementDefaultContext: 1
PredicatedContext: 3
MultipartIdentifierContext: 1
RegularQuerySpecificationContext: 1
RealIdentContext: 3
NamedExpressionSeqContext: 1
ErrorCapturingIdentifierContext: 3
RelationContext: 1
SelectClauseContext: 1
FunctionNameContext: 1
QueryPrimaryDefaultContext: 1
IdentifierContext: 8
CompoundOrSingleStatementContext: 1
FromClauseContext: 1
SingleStatementContext: 1
TableNameContext: 1
IdentifierReferenceContext: 1
UnquotedIdentifierContext: 1
QueryContext: 1
NamedExpressionContext: 2
ValueExpressionDefaultContext: 3
```

## [WIP] Use CountSparkQLRules for Probabalistic Grammar Comparison
The idea behind this tool is to parse a group of SparkQL queries and collect how many times each node  type was counted as well as parent node information. Then we could reconstruct the rule expansions seen  and normalize per parent node to determine probabilities of each rule. Unfortunately, this idea was not  explored very far as the PySpark-executable TPC-DS queries would fail to parse. The example output came  from testing a query from this [repo](https://github.com/agirish/tpcds) but it is unclear if these queries were actually generated using TPC-DS as suggested. 