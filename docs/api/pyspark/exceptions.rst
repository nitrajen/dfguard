Exceptions
==========

All frameguard exceptions inherit from ``DfTypesError``, so you can catch
everything with a single ``except DfTypesError`` if needed.

.. code-block:: python

   import frameguard.pyspark as fg
   from frameguard.pyspark.exceptions import DfTypesError, SchemaValidationError
   from pyspark.sql import SparkSession, functions as F, types as T

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )

   class OrderSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       revenue:  T.DoubleType()   # not yet in raw_df

   ds = fg.dataset(raw_df)

   try:
       ds.validate(OrderSchema)
   except SchemaValidationError as e:
       print(e.errors)    # list of field-level mismatches
       print(e.history)   # full schema history up to the failure

----

.. autoclass:: frameguard.pyspark.exceptions.DfTypesError
   :members:

.. autoclass:: frameguard.pyspark.exceptions.SchemaValidationError
   :members:

.. autoclass:: frameguard.pyspark.exceptions.TypeAnnotationError
   :members:

.. autoclass:: frameguard.pyspark.exceptions.ColumnNotFoundError
   :members:
