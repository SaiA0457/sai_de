# Databricks notebook source
from pyspark.sql.functions import * 

# Create first DataFrame
data1 = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "David"),
    (5, "Eva")
]
df1 = spark.createDataFrame(data1, ["id", "name"])

# COMMAND ----------

display(df1)

# COMMAND ----------

df1 = df1.withColumn('Flag',lit('Yes'))

# COMMAND ----------

display(df1)

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

# COMMAND ----------

df1.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------

df2 = df1.filter(col('id')==1)

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.explain()

# COMMAND ----------

df1.unpersist()

# COMMAND ----------

df2.display()

# COMMAND ----------


