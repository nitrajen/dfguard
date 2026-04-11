<div align="center">

# frameguard

**Catch DataFrame schema mismatches at the function call, not deep in your pipeline.**

[![PyPI](https://img.shields.io/pypi/v/frameguard?color=blue&label=PyPI)](https://pypi.org/project/frameguard/)
[![Python](https://img.shields.io/pypi/pyversions/frameguard)](https://pypi.org/project/frameguard/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![CI](https://github.com/nitrajen/frameguard/actions/workflows/ci.yml/badge.svg)](https://github.com/nitrajen/frameguard/actions)
[![Coverage](https://codecov.io/gh/nitrajen/frameguard/branch/main/graph/badge.svg)](https://codecov.io/gh/nitrajen/frameguard)
[![Docs](https://img.shields.io/badge/docs-frameguard.readthedocs.io-blue)](https://frameguard.readthedocs.io)

</div>

---

Data pipelines fail late. You pass the wrong DataFrame into a function, the job runs, and eventually crashes with an error pointing at the wrong place. The actual bug was earlier, at the function call.

**frameguard moves that failure to the function call.** The wrong DataFrame is rejected immediately with a precise error: which function, which argument, what schema was expected, what arrived.

Currently supports PySpark. pandas and polars support coming soon.

```python
import frameguard.pyspark as fg
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
raw_df = spark.createDataFrame(
    [(1, 10.0, 3), (2, 5.0, 7)],
    "order_id LONG, amount DOUBLE, quantity INT",
)

RawSchema = fg.schema_of(raw_df)

@fg.enforce
def enrich(df: RawSchema):
    return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

EnrichedSchema = fg.schema_of(enrich(raw_df))

@fg.enforce
def flag_high_value(df: EnrichedSchema):
    return df.withColumn("is_vip", F.col("revenue") > 1000)

flag_high_value(raw_df)
# TypeError: Schema mismatch in flag_high_value() argument 'df':
#   expected: order_id:long, amount:double, quantity:int, revenue:double
#   received: order_id:long, amount:double, quantity:int
```

No validation logic inside functions. The wrong DataFrame simply cannot enter the wrong function.

---

## Install

```bash
pip install frameguard[pyspark]
```

Requires Python >= 3.10, PySpark >= 3.3. No other dependencies.

---

## Two ways to define a schema

### Capture from a live DataFrame

```python
RawSchema      = fg.schema_of(raw_df)       # exact snapshot of this stage
EnrichedSchema = fg.schema_of(enriched_df)  # new type after adding columns
```

Exact matching: a DataFrame with extra columns does **not** satisfy `RawSchema`. Capture a new type at each stage boundary.

### Declare upfront

```python
from frameguard.pyspark import Optional
from pyspark.sql import types as T

class OrderSchema(fg.SparkSchema):
    order_id: T.LongType()
    amount:   T.DoubleType()
    tags:     T.ArrayType(T.StringType())
    address:  Optional[T.StringType()]   # nullable field

class EnrichedSchema(OrderSchema):       # inherits all parent fields
    revenue: T.DoubleType()
```

No live DataFrame needed. Subclasses inherit parent fields. Works with nested structs, arrays, and maps.

Use the schema when creating a DataFrame:

```python
df = spark.createDataFrame(rows, OrderSchema.to_struct())
```

---

## Enforcement

### Arm once, protect everything

```python
# my_pipeline/__init__.py
import frameguard.pyspark as fg

fg.arm()   # walks the package, wraps every annotated function
```

Node functions need no decorator. The type annotation is the contract:

```python
# my_pipeline/nodes.py
def enrich(df: OrderSchema):       # enforced automatically
    return df.withColumn(...)

def aggregate(df: EnrichedSchema): # also enforced
    return df.groupBy(...)
```

### Per-function decoration

```python
@fg.enforce                   # subset=True: extra columns fine (default)
def process(df: OrderSchema): ...

@fg.enforce(subset=False)     # exact match: no extra columns allowed
def write_final(df: OrderSchema): ...

@fg.enforce(always=True)      # enforces even after fg.disable()
def critical(df: OrderSchema): ...
```

### The `subset` flag

| Level | How | Default |
|---|---|---|
| Global | `fg.arm(subset=True/False)` | `True` |
| Function | `@fg.enforce(subset=True/False)` | inherits global |

Function level always wins. `schema_of` types always use exact matching regardless of `subset`.

---

## Validate at load time

Use `assert_valid` right after reading from storage to catch upstream schema drift before processing starts:

```python
raw = spark.read.parquet("/data/orders/raw.parquet")
OrderSchema.assert_valid(raw)   # raises SchemaValidationError with full diff if schema changed

enriched = enrich(raw)          # @fg.enforce then guards the function call
```

Reports all problems at once, not just the first:

```
SchemaValidationError: Schema validation failed:
  Column 'revenue': type mismatch: expected double, got string
  Missing column 'is_high_value' (expected boolean, nullable=False)
```

---

## Schema history

`fg.dataset(df)` records every schema-changing operation. When validation fails, the error includes the full history so you know exactly where the schema diverged.

```python
ds = fg.dataset(raw_df)
ds = ds.withColumn("revenue",  F.col("amount") * 1.1)
ds = ds.withColumn("discount", F.when(F.col("revenue") > 500, 50.0).otherwise(0.0))
ds = ds.drop("tags")
ds = ds.withColumnRenamed("customer", "customer_name")

print(ds.schema_history)
# [0] input                    order_id:long, customer:string, amount:double, ...
# [1] withColumn('revenue')    + revenue:double
# [2] withColumn('discount')   + discount:double
# [3] drop(['tags'])           - tags
# [4] withColumnRenamed(...)   customer -> customer_name
```

---

## Pipeline integrations

frameguard fits naturally into pipeline frameworks. See the full docs for working examples:

- **[Airflow](https://frameguard.readthedocs.io/en/latest/airflow.html)**: `@fg.enforce` on transform helpers + `assert_valid` after loading from storage
- **[Kedro](https://frameguard.readthedocs.io/en/latest/kedro.html)**: `fg.arm()` in one place, node functions need no decorators

---

## Documentation

**[Quickstart](https://frameguard.readthedocs.io/en/latest/quickstart.html)**: nested structs, multi-stage pipelines, subset flag, schema history

- [API reference](https://frameguard.readthedocs.io/en/latest/api/index.html): `arm`, `enforce`, `schema_of`, `SparkSchema`, `dataset`
- [Airflow integration](https://frameguard.readthedocs.io/en/latest/airflow.html)
- [Kedro integration](https://frameguard.readthedocs.io/en/latest/kedro.html)

---

## License

[Apache 2.0](LICENSE)
