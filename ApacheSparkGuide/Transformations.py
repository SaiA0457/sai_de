# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

data = [
    ("Alice", 25, "New York"),
    ("Bob", 30, "San Francisco"),
    ("Charlie", 35, "Chicago")
]

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Narrow Transformations**

# COMMAND ----------

df = df.filter(col('city')=='New York')

# COMMAND ----------

display(df)

# COMMAND ----------

df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Wide Transformation**

# COMMAND ----------

df = df.groupBy('city').agg(max(col('age')))

# COMMAND ----------

display(df)

# COMMAND ----------

df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Repartition VS Coalesce**

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# Repartition

df = df.repartition(3)

# COMMAND ----------

# Coalesce

df = df.coalesce(1)

# COMMAND ----------

df.explain()

# COMMAND ----------


