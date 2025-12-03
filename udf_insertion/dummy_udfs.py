"""Collection of small dummy UDFs with a variety of type signatures.

These are plain Python functions (safe to import without PySpark).
Also provides helpers to lazily wrap any function as a PySpark UDF when
PySpark is available (`make_pyspark_udf`). This module is intended for
generating function ASTs (e.g. via `inspect.getsource` + `libcst.parse_module`)
or for quickly registering UDFs in a Spark session.

Usage examples (see README or bottom of file):
  - Import functions and use `inspect.getsource(func)` to get source string
  - Use `libcst.parse_module(source)` to obtain a libcst AST for injection
  - If you have PySpark available, call `make_pyspark_udf(func, return_type)`
    to receive a `pyspark.sql.column.Column`-ready UDF wrapper

This file avoids importing PySpark at module import-time to remain lightweight.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date
from decimal import Decimal
import inspect
import math
import json
import base64
import re
import uuid
import html

# Optional heavy deps commonly referenced in PySpark code samples. Import
# lazily/fail-soft so this module remains cheap to import during analysis.
try:
    import numpy as np
except Exception:
    np = None

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import pyspark.sql.functions as F  # type: ignore
    import pyspark.sql.types as T  # type: ignore
    from pyspark.sql.types import (
        StringType,
        IntegerType,
        BooleanType,
        DateType,
        TimestampType,
        DecimalType,
    )
except Exception:
    F = None
    T = None
    StringType = IntegerType = BooleanType = DateType = TimestampType = DecimalType = None

__all__ = [
    # functions
    "add",
    "concat",
    "timestamp_to_date",
    "to_decimal_str",
    "unescape_html",
    "regex_replace",
    "kw_only_scale",
    # helpers
    "make_pyspark_udf",
    "all_simple_functions",
]


def add(a: int, b: int) -> int:
    """Add two integers and return an int."""
    return a + b


def concat(a: str, b: str) -> str:
    """Concatenate two strings."""
    return f"{a}{b}"


def timestamp_to_date(ts: datetime) -> date:
    """Convert a datetime to a date (strip time part)."""
    return ts.date()


def to_decimal_str(x: Decimal) -> str:
    """Format a Decimal to a two-decimal string representation."""
    return format(x.quantize(Decimal("0.01")), "f")


def kw_only_scale(a: int, *, scale: int = 2) -> int:
    """Multiply `a` by keyword-only `scale` parameter."""
    return a * scale


def unescape_html(s: str) -> str:
    """Return the HTML-unescaped version of `s` using `html.unescape`."""
    return html.unescape(s)


def regex_replace(s: str, pattern: str, repl: str) -> str:
    """Replace occurrences matching `pattern` in `s` with `repl` using `re.sub`."""
    return re.sub(pattern, repl, s)
# A small registry of the functions above for convenience
all_simple_functions = [
    add,
    concat,
    timestamp_to_date,
    to_decimal_str,
    unescape_html,
    regex_replace,
    kw_only_scale,
]


def make_pyspark_udf(func, return_type=None):
    """Lazily wrap `func` as a PySpark UDF.

    - If PySpark is not importable, raises ImportError with a helpful message.
    - `return_type` should be a PySpark DataType instance (e.g. `StringType()`),
      or None to let PySpark infer.

    Example:
        from pyspark.sql.types import IntegerType
        spark_udf = make_pyspark_udf(add_ints, IntegerType())

    Note: we import PySpark only when this function is called to avoid
    forcing heavy dependencies during import-time analysis.
    """
    try:
        import pyspark.sql.functions as F  # type: ignore
    except Exception as exc:  # ImportError or other PySpark import issues
        raise ImportError(
            "PySpark is not available in this environment. "
            "Install pyspark or call this only in a Spark-enabled runtime."
        ) from exc

    # If user passed an actual pyspark type object, just use it
    # F.udf allows None as the returnType for inference.
    return F.udf(func, return_type)


def get_source_as_libcst_module(func):
    """Return a libcst.Module parsed from the source of `func`.

    Requires `libcst` to be installed in the environment that calls this.
    This function does not import `libcst` at module-import time.
    """
    try:
        import libcst as cst  # type: ignore
    except Exception as exc:
        raise ImportError(
            "libcst is required to parse function source into an AST. "
            "Install `libcst` to use this helper."
        ) from exc

    src = inspect.getsource(func)
    # Wrap source in a module parse to get a Module node
    return cst.parse_module(src)


if __name__ == "__main__":
    # Quick demonstration that importing is cheap and the helpers work
    print("Available dummy UDFs:")
    for f in all_simple_functions:
        print(f"- {f.__name__}{inspect.signature(f)}")
