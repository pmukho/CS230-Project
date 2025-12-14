# SQL to PySpark Translator

This folder contains tools to translate SQL queries into PySpark DataFrame API code. It includes a core translator, comprehensive test suites, and utilities for working with translated queries.

## Files Overview

### Core Translator

#### `translator.py`
**Main translation engine** that converts SQL queries to PySpark code.

**Key Features:**
- Parses SQL queries using `mo_sql_parsing` library
- Translates common SQL operations to PySpark equivalents:
  - `SELECT`, `FROM`, `WHERE` → `.select()`, `.filter()`
  - Aggregations (`SUM`, `AVG`, `COUNT`, etc.) → `.groupBy().agg()`
  - `JOIN` operations → `.join()`
  - `ORDER BY` → `.orderBy()`, `LIMIT` → `.limit()`
  - Window functions → `.over()` with `Window.partitionBy()`, `Window.orderBy()`
  - Set operations (`UNION`, `INTERSECT`, `EXCEPT`)
  - Subqueries in `SELECT`, `WHERE`, and `FROM` clauses
  - `CASE WHEN` expressions → PySpark conditionals
  - String functions (`UPPER`, `LOWER`, `SUBSTRING`, etc.)
  - Date functions (`DATE_ADD`, `YEAR`, `MONTH`, `DAY`, etc.)

**Usage:**
```python
from translator import translate_sql_to_pyspark

sql_query = "SELECT name, salary FROM employees WHERE salary > 50000"
pyspark_code = translate_sql_to_pyspark(sql_query)
print(pyspark_code)
# Output: df = spark.table("employees").filter(col("salary") > 50000).select("name", "salary")
```

**Supported SQL Constructs:**
- Basic: `SELECT`, `FROM`, `WHERE`, `DISTINCT`, `LIMIT`, `ORDER BY`
- Aggregation: `GROUP BY`, `HAVING`, aggregate functions
- Joins: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`
- Subqueries: correlated and non-correlated in multiple contexts
- Window functions: `OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)`
- Set operations: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
- Conditionals: `CASE WHEN`, `COALESCE`, `NULLIF`, `IS NULL`, `IS NOT NULL`
- Functions: arithmetic, string manipulation, date operations, type casting

### Test Files

#### `tests_gpt.py`
**Comprehensive parametrized test suite** with 100+ test cases covering all major SQL features.

**Test Categories:**
1. Basic SELECT/FROM/WHERE operations
2. DISTINCT, LIMIT, ORDER BY, GROUP BY, HAVING
3. Aggregation functions
4. Arithmetic and logical operators
5. String and date functions
6. Conditional expressions (CASE, COALESCE, NULLIF)
7. All types of JOINs
8. Subqueries
9. Set operations
10. Window functions
11. Complex nested expressions
12. Aliases and escaping
13. Edge cases (incomplete queries, empty input, comments)
14. Full complex real-world queries

**Running Tests:**
```bash
# Run all tests
pytest tests_gpt.py -v

# Run specific test category
pytest tests_gpt.py::test_basic_select_from_where -v

# Run with coverage
pytest tests_gpt.py --cov=translator --cov-report=html
```

**Test Features:**
- Validates output is valid Python code (using `ast.parse()`)
- Checks for presence of PySpark methods (`.select()`, `.filter()`, etc.)
- Parametrized tests allow testing many variations of similar constructs
- Helper function `assert_valid_translation()` ensures both syntax validity and PySpark presence

#### `test_sql_frame.py`
**SQLFrame integration test** demonstrating how to validate translated code against an actual PySpark-compatible backend.

**Purpose:**
- Tests the generated PySpark code against SQLFrame (a PySpark-compatible SQL execution engine)
- Creates in-memory DataFrames without needing a Spark cluster
- Useful for end-to-end validation of translations

**Usage:**
```bash
pytest test_sql_frame.py -v
```

### Sample Data and Translations

#### `transplanted_query1.sql`
Original SQL query used for testing and demonstration.

#### `translated_transplanted_query1.txt`
Expected PySpark translation output for `transplanted_query1.sql`.

#### `translations_1_22.txt`
Batch of SQL to PySpark translations (queries 1-22).

## Installation & Setup

### Requirements
Install dependencies from the project root:
```bash
pip install -r ../requirements.txt
```

Key dependencies:
- `mo-sql-parsing`: SQL parser
- `pytest`: Test framework
- `sqlframe`: SQLFrame for testing (optional, for `test_sql_frame.py`)

### Quick Start

**1. Translate a single SQL query:**
```python
from sql_to_pyspark.translator import translate_sql_to_pyspark

query = "SELECT * FROM users WHERE age > 18 ORDER BY name LIMIT 10"
result = translate_sql_to_pyspark(query)
print(result)
```

**2. Translate from a file:**
```python
def translate_file(sql_file_path):
    with open(sql_file_path, 'r') as f:
        query = f.read()
    return translate_sql_to_pyspark(query)

pyspark_code = translate_file('transplanted_query1.sql')
```

**3. Run the test suite:**
```bash
cd /Users/henry/CS230-Project
pytest sql_to_pyspark/tests_gpt.py -v --tb=short
```

## How Translations Work

### Translation Pipeline

1. **Parsing**: `mo_sql_parsing.parse()` converts SQL to a dictionary structure
2. **Analysis**: Detect aggregates, window functions, subqueries, and scalar correlations
3. **Translation**: Helper functions (`fn_select`, `fn_where`, `fn_groupby`, etc.) convert SQL AST to PySpark code
4. **Assembly**: Final PySpark code is assembled with proper method chaining

### Key Translation Patterns

| SQL | PySpark |
|-----|---------|
| `SELECT a, b` | `.select(col("a"), col("b"))` |
| `WHERE x > 5` | `.filter(col("x") > 5)` |
| `GROUP BY x` | `.groupBy(col("x"))` |
| `COUNT(*)` | `.agg(func.count(lit(1)))` |
| `ORDER BY x DESC` | `.orderBy(col("x").desc())` |
| `JOIN ... ON` | `.join(other_df, on_condition)` |
| `SELECT ... OVER (PARTITION BY ...)` | `.over(Window.partitionBy(...))` |

### Limitations & Known Issues

1. **CTEs (WITH clause)**: Partially supported; may not handle complex recursive CTEs
2. **Correlated Subqueries**: Supported via JOIN translation; some edge cases may not translate perfectly
3. **Complex Functions**: Custom SQL functions not in the mapping may fall back to string representation
4. **Type Casting**: `CAST` is supported but type compatibility depends on PySpark target
5. **Spark SQL Fallback**: Complex queries that cannot be fully translated use `spark.sql()` wrapper as fallback

## Development & Contribution

### Code Structure

**`translator.py` Organization:**
- **Parsing helpers**: `parse_token_file()`, condition parsing
- **Function translators**: `fn_select()`, `fn_where()`, `fn_groupby()`, etc.
- **Expression translators**: `translate_condition()`, `translate_value()`, `translate_function()`
- **Special handlers**: Window functions, window specs, scalar subqueries, set operations
- **Main entry point**: `fn_genSQL()` orchestrates the full translation

### Adding Support for New SQL Features

1. Identify the SQL feature in the parsed dictionary structure
2. Add a handler function (e.g., `fn_your_feature()`)
3. Call it from `fn_genSQL()` at the appropriate point
4. Add test cases in `tests_gpt.py`
5. Test with `pytest`

### Example: Adding a New Function

```python
# In translator.py, add to SPECIAL_FUNCTION_MAPPING:
SPECIAL_FUNCTION_MAPPING = {
    "date_add": "dateadd",
    "str_to_date": "to_date",
    "substr": "substring",
    "my_custom_func": "pyspark_equivalent",  # Add here
}

# Add test case in tests_gpt.py:
@pytest.mark.parametrize("query", [
    "SELECT my_custom_func(col1) FROM table1",
])
def test_custom_function(query):
    assert_valid_translation(query)
```