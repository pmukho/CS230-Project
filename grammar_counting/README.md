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
└──  src
    ├── TestSQLParse.java
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

## TODO
```
$mkdir -p build
$javac -d build -cp /usr/local/lib/antlr-4.13.2-complete.jar src/org/example/*.java src/TestSQLParse.java
$java -cp build:/usr/local/lib/antlr-4.13.2-complete.jar TestSQLParse <your input file>
```
