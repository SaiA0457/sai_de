# Databricks notebook source
spark.conf.set("spark.sql.adaptive.enabled",False)

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# Data Reading

df = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("/FileStore/ApacheSpark/MegaMart.csv")

# Transformations

df = df.groupBy('product_name').agg(count(col('order_id')))

# Action 

display(df)

# COMMAND ----------


