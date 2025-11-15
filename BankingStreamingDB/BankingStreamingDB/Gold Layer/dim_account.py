# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

silver_path = "abfss://silver@storage4fintechsd.dfs.core.windows.net/account/"
df_acc = spark.read.format("parquet").load(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Currency conversion mock rates (Gold layer usually includes this table)
# MAGIC

# COMMAND ----------

fx_rates = {
    "USD": 1.0,
    "EUR": 1.08,   # EUR -> USD
    "PKR": 0.0036  # PKR -> USD
}

# COMMAND ----------

dim_account = df_acc.withColumn("created_date", F.to_date("created_at")) \
               .withColumn("is_active_flag", F.when(F.col("status") == "active", 1).otherwise(0)) \
               .withColumn("risk_level",F.when(F.col("status") == "blocked", "HIGH")
                                         .when(F.col("status") == "inactive", "MEDIUM")
                                         .otherwise("LOW")) \
                .withColumn("balance_usd",F.col("balance") * F.create_map([F.lit(x) for x in sum(fx_rates.items(), ())])[F.col("currency")])\
                .drop("balance", "created_date", "is_active_flag", "balance_usd")

# COMMAND ----------

display(dim_account)

# COMMAND ----------

dim_account.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/dim_account")
