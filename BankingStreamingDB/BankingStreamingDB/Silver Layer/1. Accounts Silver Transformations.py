# Databricks notebook source
# MAGIC %md
# MAGIC ## `Accounts Silver Transformations`

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

bronze_path = "abfss://bronze@storage4fintechsd.dfs.core.windows.net/accounts/"
df = spark.read.format("parquet").load(bronze_path)

# COMMAND ----------

df = df.withColumn(
    "is_json",
    col("raw_data").rlike(r"^\s*\{.*\}\s*$")
)


# COMMAND ----------

# JSON schema for accounts
account_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("status", StringType(), True),
])

# COMMAND ----------

# parse JSON
json_df = df.withColumn(
    "json_parsed",
    from_json(col("raw_data"), account_schema)
)

# Extract into proper columns
df_account = json_df.select(
    col("raw_data"),
    col("is_json"),
    col("json_parsed.*")  # expands all JSON keys
)

# COMMAND ----------

df_account = df_account.drop("raw_data", "is_json")\
                       .dropna(subset=["account_id", "customer_id"],how='all')\
                       .dropDuplicates(subset=["account_id"])\
                       .withColumn("created_at",to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))

display(df_account)

# COMMAND ----------

df_account.write.mode("overwrite").format("parquet").save("abfss://silver@storage4fintechsd.dfs.core.windows.net/account/")