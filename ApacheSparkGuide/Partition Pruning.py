# Databricks notebook source
from pyspark.sql import SparkSession


# Step : Sample data
data = [
    ("Alice", "HR", 1000),
    ("Bob", "IT", 2000),
    ("Charlie", "HR", 1500),
    ("David", "Finance", 2500),
    ("Eve", "IT", 1800),
    ("Frank", "Finance", 2200)
]

columns = ["name", "department", "salary"]

# Step : Create DataFrame
df = spark.createDataFrame(data, columns)

# Step : Write DataFrame using partitioning
output_path = "/FileStore/ApacheSpark/OutputData"  

df.write \
  .mode("overwrite") \
  .partitionBy("department") \
  .parquet(output_path)



# COMMAND ----------

output_path_new = "/FileStore/ApacheSpark/OutputDataWithout"  

df.write \
  .mode("overwrite") \
  .parquet(output_path_new)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_withpart = spark.read.format("parquet")\
                .load(output_path)

# COMMAND ----------

display(df_withpart)

# COMMAND ----------

df_withoutpart = spark.read.format("parquet")\
                .load(output_path_new)\
                .filter(col('department')=='HR')

# COMMAND ----------

display(df_withoutpart)

# COMMAND ----------

# MAGIC %md
# MAGIC **Dynamic Partition Pruning**

# COMMAND ----------

df_join_new = df_withoutpart.join(df_withpart, (df_withoutpart['name']==df_withpart['name']) & (df_withoutpart['department']==df_withpart['department']), how='inner')

# COMMAND ----------

display(df_join_new)

# COMMAND ----------


