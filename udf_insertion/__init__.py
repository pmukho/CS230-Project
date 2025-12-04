"""udf_insertion package initializer.

Makes the submodules `dummy_udfs` and `inserter` available for simple imports
like `import udf_insertion.dummy_udfs` or `from udf_insertion import dummy_udfs`.

This file intentionally keeps imports lightweight so importing the package
does not pull heavy optional deps at module import time.
"""
from . import dummy_udfs
from . import inserter

__all__ = [
    "dummy_udfs",
    "inserter",
]
