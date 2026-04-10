Decorators
==========

These decorators attach schema validation explicitly to a function's
input or output, using ``fg.SparkSchema`` classes you declare upfront.
They are an alternative to the ``@fg.enforce`` + annotation approach,
useful when you want explicit validation logic that is separate from
the type annotation.

check_schema
------------

Validates only the **return value**. Use this when the input does not
need guarding but you want to guarantee the output shape.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, functions as F, types as T

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3), (2, 5.0, 7)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )

   class EnrichedSchema(fg.SparkSchema):
       order_id: T.LongType()
       revenue:  T.DoubleType()

   @fg.check_schema(EnrichedSchema)
   def enrich(df):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   enrich(raw_df)   # OK: returned DataFrame has order_id and revenue
   # If revenue were missing, raises SchemaValidationError.

.. autofunction:: frameguard.pyspark.decorators.check_schema

----

typed_transform
---------------

Validates both the **input** and the **return value**. Use this for
functions at critical pipeline boundaries where both sides matter.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, functions as F, types as T

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3), (2, 5.0, 7)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )

   class RawSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       quantity: T.IntegerType()

   class EnrichedSchema(RawSchema):
       revenue: T.DoubleType()

   @fg.typed_transform(input_schema=RawSchema, output_schema=EnrichedSchema)
   def enrich(df):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   enrich(raw_df)   # OK: input and output both validated
   # Wrong input schema raises on the way in.
   # Wrong output schema raises on the way out.

.. autofunction:: frameguard.pyspark.decorators.typed_transform
