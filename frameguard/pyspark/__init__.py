"""
frameguard.pyspark: runtime schema enforcement for PySpark DataFrames.

Two-line setup for packages (Kedro, Airflow, any importable module)
-------------------------------------------------------------------
::

    from frameguard.pyspark import schema_of, arm

    RawSchema = schema_of(raw_df)

    def enrich(df: RawSchema): ...
    def clean(df: RawSchema): ...

    arm()                              # enforces every annotated function above

For scripts and notebooks use ``@enforce`` per function::

    from frameguard.pyspark import schema_of, enforce

    RawSchema = schema_of(raw_df)

    @enforce
    def enrich(df: RawSchema): ...

Declaring schemas upfront (no live DataFrame required)
------------------------------------------------------
::

    from pyspark.sql import types as T
    from typing import Optional
    from frameguard.pyspark import SparkSchema, enforce

    class OrderSchema(SparkSchema):
        order_id: T.LongType()
        amount:   T.DoubleType()
        tags:     T.ArrayType(T.StringType())
        address:  AddressSchema            # nested struct
        zip:      Optional[T.StringType()] # nullable

    @enforce
    def process(df: OrderSchema): ...      # subset matching: df must have these fields

Public API
----------
``schema_of(df)``
    Capture ``df``'s schema as a type.  Exact match required.

``dataset(df)``
    Wrap ``df`` in a tracked instance.  Every ``withColumn``, ``drop``,
    ``select``, etc. is recorded in ``schema_history``.

``arm()``
    Apply schema enforcement to every annotated function in the calling
    module.  Call after all function definitions.

``enforce``
    Per-function decorator.  Only checks schema-annotated args.

``SparkSchema``
    Declare a schema as a class using real PySpark types.
    Subset matching: df must have at least the declared fields.

``check_schema(schema)`` / ``typed_transform(input_schema, output_schema)``
    Function decorators for explicit input/output validation.
"""

try:
    import pyspark  # noqa: F401
except ImportError as _e:
    raise ImportError(
        "frameguard's PySpark integration requires PySpark. "
        "Install it with: pip install 'frameguard[pyspark]'"
    ) from _e

from frameguard.pyspark._enforcement import arm, disable, enable_enforcement, enforce
from frameguard.pyspark._inference import infer_schema
from frameguard.pyspark.coercion import result_type
from frameguard.pyspark.dataset import TypedGroupedData, _TypedDatasetBase, schema_of
from frameguard.pyspark.dataset import _make_dataset as dataset
from frameguard.pyspark.decorators import check_schema, typed_transform
from frameguard.pyspark.exceptions import (
    ColumnNotFoundError,
    DfTypesError,
    SchemaValidationError,
    TypeAnnotationError,
)
from frameguard.pyspark.history import SchemaChange, SchemaHistory
from frameguard.pyspark.schema import SparkSchema  # noqa: E402

__all__ = [
    "schema_of",
    "dataset",
    "enforce",
    "arm",
    "disable",
    "enable_enforcement",
    "_TypedDatasetBase",
    "TypedGroupedData",
    "SparkSchema",
    "typed_transform",
    "check_schema",
    "SchemaChange",
    "SchemaHistory",
    "DfTypesError",
    "SchemaValidationError",
    "TypeAnnotationError",
    "ColumnNotFoundError",
    "infer_schema",
    "result_type",
]
