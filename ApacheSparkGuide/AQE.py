# Databricks notebook source
spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",True)

# COMMAND ----------

data = [
    ("Alice", "HR", 1000),
    ("Bob", "IT", 2000),
    ("Charlie", "HR", 1500),
    ("David", "Finance", 2500),
    ("Eve", "IT", 1800),
    ("Frank", "Finance", 2200)
]

columns = ["name", "department", "salary"]

df = spark.createDataFrame(data, columns)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.groupBy('department').agg(count('name'))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **JOINS**

# COMMAND ----------

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

df_new = df1.join(df2, df1['id']==df2['id'],how='inner')
display(df_new)

# COMMAND ----------


