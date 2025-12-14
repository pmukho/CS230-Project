# UDF Insertion

SEE ../sql_annotated/schema_aware_sql.ipynb FOR SQL TRANSPLANTING OF UDFS. This insertion.py method is unused in our implmenetation.

This folder contains utilities for analyzing Python code ASTs and inserting calls to User-Defined Functions (UDFs) based on type annotations.

## Files

- `inserter.py`: Main AST transformation script using `libcst`. Scans Python code for annotated variable assignments (`x: T = call(...)`), infers argument types from literals or previously annotated variables, matches them against available UDFs in `dummy_udfs.py`, and rewrites matched calls to invoke the corresponding UDFs. Returns a modified AST with import statements automatically added. The primary function is `insert_udfs_by_types(module: cst.Module, n: int)`.

- `dummy_udfs.py`: Collection of 15+ pure Python functions with explicit type annotations. Includes utilities like `add`, `concat`, `timestamp_to_date`, `to_decimal_str`, `unescape_html`, `regex_replace`, `split_to_list`, `map_to_upper`, `bytes_to_base64`, and more. These are deterministic, safe to import without PySpark, and can be wrapped as PySpark UDFs via the `make_pyspark_udf(func, return_type)` helper.

## Prerequisites

- Python 3.10+
- `libcst` for AST parsing and transformation (`pip install libcst`)

## Quick Start

### 1. Basic AST Transformation

Use `inserter.py` to transform annotated Python code and inject UDF calls:

```python
import libcst as cst
from udf_insertion.inserter import insert_udfs_by_types

# Parse some annotated Python code
code = '''
x: int = some_func(1, 2)
y: str = some_func(x, 'hello')
'''
module = cst.parse_module(code)

# Transform up to 2 annotated calls to inject UDFs
new_module = insert_udfs_by_types(module, n=2, seed=42)

# Print the modified code
print(new_module.code)
```

Expected output (if matching UDFs exist):
```python
from udf_insertion.dummy_udfs import add
x: int = add(1, 2)
y: str = some_func(x, 'hello')
```

### 2. Demo Script

Run the built-in demo to see the transformation in action:

```bash
cd /Users/henry/CS230-Project/udf_insertion
python inserter.py
```

This runs `_demo()` which transforms a sample code snippet and prints the result.

### 3. Use in Your Code

```python
from udf_insertion.dummy_udfs import add, concat
import libcst as cst
from udf_insertion.inserter import insert_udfs_by_types

# Define some Python code with annotations
code = '''
result: int = compute(5, 10)
message: str = build_msg('Hello', 'World')
'''

# Parse and transform
module = cst.parse_module(code)
transformed = insert_udfs_by_types(module, n=10)

# Use the transformed code
print(transformed.code)
```

## How It Works

1. **AST Parsing**: `inserter.py` uses `libcst` to parse Python code into an Abstract Syntax Tree.

2. **Annotation Collection**: Walks the AST to collect all variable type annotations and their names (e.g., `x: int` → `{'x': 'int'}`).

3. **Candidate Detection**: Identifies annotated assignment nodes where the value is a function call.

4. **Type Inference**: For each call argument, infers the type from:
   - Literal values (e.g., `1` → `int`, `'str'` → `str`)
   - Previously annotated variables (e.g., `x` where `x: int`)

5. **UDF Matching**: Builds a registry of available UDFs from `dummy_udfs.py` mapping `(arg_types..., return_type) → udf_names`. For each candidate, looks up matching UDFs with compatible signatures.

6. **Code Rewriting**: Replaces up to `n` matched calls with actual UDF invocations.

7. **Import Addition**: Automatically adds `from udf_insertion.dummy_udfs import <udf_name>` statements for any UDFs used.

## Available UDFs

- **`add(a: int, b: int) -> int`**: Add two integers.
- **`concat(a: str, b: str) -> str`**: Concatenate two strings.
- **`timestamp_to_date(ts: datetime) -> date`**: Convert datetime to date.
- **`to_decimal_str(x: Decimal) -> str`**: Format Decimal to string.
- **`regex_replace(s: str, pattern: str, repl: str) -> str`**: Replace pattern in string.
- **`split_to_list(s: str, sep: Optional[str] = None) -> List[str]`**: Split string into list.
- **`map_to_upper(items: List[str]) -> List[str]`**: Map strings to uppercase.
- **`bytes_to_base64(b: bytes) -> str`**: Encode bytes to base64.
- **`unescape_html(s: str) -> str`**: Unescape HTML entities.
- **`to_float_or_none(s: Optional[str]) -> Optional[float]`**: Parse string to float or None.
- And more: `sum_vars`, `gen_id_list`, `dummy_identity`, `bool_flag`, `kw_only_scale`.

See `dummy_udfs.py` for complete signatures and implementations.

## Notes

- **Type Matching**: The insertion is type-aware. UDFs are matched based on argument and return types. Mismatches are skipped.
- **Positional Arguments**: The script prefers UDFs callable with positional arguments matching the call sites found.
- **Conservative Approach**: Only simple, statically-annotated patterns (literals and annotated variables as arguments) are supported.
- **Randomization**: By default, if multiple UDFs match, one is randomly chosen. Pass `seed` to `insert_udfs_by_types` for reproducibility.
- **PySpark Integration**: Use `dummy_udfs.make_pyspark_udf(func, return_type)` to wrap any function as a PySpark UDF for use in Spark DataFrames.

## Example: PySpark Integration (Optional)

If you have PySpark available:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from udf_insertion.dummy_udfs import add, make_pyspark_udf

spark = SparkSession.builder.appName("test").getOrCreate()

# Wrap add as a PySpark UDF
add_udf = make_pyspark_udf(add, IntegerType())

# Use in a DataFrame
df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
result = df.withColumn("sum", add_udf("a", "b"))
result.show()
```

## Next Steps

- Extend `dummy_udfs.py` with additional functions for your project.
- Enhance `inserter.py` to support more complex pattern matching (e.g., nested calls, function returns).
- Add unit tests to validate type inference and UDF matching logic.
