# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Lazy Evaluation & Action**

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

df_new = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

df_new = df_new.filter(col('city')=='New York')

# COMMAND ----------

df_new = df_new.select('city')

# COMMAND ----------

display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **Query Plans**

# COMMAND ----------

df_new.explain()

# COMMAND ----------


