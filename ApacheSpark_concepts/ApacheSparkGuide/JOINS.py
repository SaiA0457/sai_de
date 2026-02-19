# Databricks notebook source
spark.conf.set("spark.sql.adaptive.enabled",False)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Create first DataFrame
data1 = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "David"),
    (5, "Eva")
]
df1 = spark.createDataFrame(data1, ["id", "name"])

# Create second DataFrame
data2 = [
    (1, 50000),
    (2, 60000),
    (3, 70000),
    (6, 80000)
]
df2 = spark.createDataFrame(data2, ["id", "salary"])


# COMMAND ----------

df_join_broad = df1.join(broadcast(df2), df1['id']==df2['id'], 'left')

# COMMAND ----------

display(df_join_broad)

# COMMAND ----------


