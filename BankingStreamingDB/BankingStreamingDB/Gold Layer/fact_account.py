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

dim_account_status = spark.createDataFrame([
    ("active",   "Account is active and usable"),
    ("inactive", "Account exists but disabled"),
    ("blocked",  "Account is restricted/flagged"),
], ["status", "status_meaning"])


# COMMAND ----------

fx_rates = {
    "USD": 1.0,
    "EUR": 1.08,   # EUR -> USD
    "PKR": 0.0036  # PKR -> USD
}

# COMMAND ----------

df_acc = df_acc.withColumn("created_date", F.to_date("created_at")) \
               .withColumn("is_active_flag", F.when(F.col("status") == "active", 1).otherwise(0)) \
               .withColumn("risk_level",F.when(F.col("status") == "blocked", "HIGH")
                                         .when(F.col("status") == "inactive", "MEDIUM")
                                         .otherwise("LOW")) \
                .withColumn("balance_usd",F.col("balance") * F.create_map([F.lit(x) for x in sum(fx_rates.items(), ())])[F.col("currency")])\
                .drop("created_date")

# COMMAND ----------

display(df_acc)

# COMMAND ----------

# MAGIC %md
# MAGIC `GOLD METRICS TABLE: agg_account_metrics`

# COMMAND ----------

# Total balance by currency
currency_balance = df_acc.groupBy("currency").agg(F.sum("balance").alias("total_balance"))

# Total accounts by status
status_counts = df_acc.groupBy("status").agg(F.count("*").alias("total_accounts"))

# Average balance by account type
avg_balance_type = df_acc.groupBy("account_type").agg(F.avg("balance").alias("average_balance"))

# Daily account openings
daily_openings = df_acc.groupBy(F.to_date("created_at").alias("date")).agg(F.count("*").alias("accounts_created"))

# COMMAND ----------

display(currency_balance)
display(status_counts)
display(avg_balance_type)
display(daily_openings)

# COMMAND ----------

currency_balance.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/currency_balance/")
status_counts.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/status_counts/")
avg_balance_type.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/avg_balance_type/")
daily_openings.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/daily_openings/")


# COMMAND ----------

currency_balance = currency_balance.withColumn("join_key", F.lit(1))
status_counts = status_counts.withColumn("join_key", F.lit(1))
avg_balance_type = avg_balance_type.withColumn("join_key", F.lit(1))
daily_openings = daily_openings.withColumn("join_key", F.lit(1))


# COMMAND ----------

from functools import reduce

agg_list = [currency_balance, status_counts, avg_balance_type, daily_openings]

fact_account_metrics = reduce(lambda df1, df2: df1.join(df2, on="join_key", how="outer"), agg_list) \
    .drop("join_key")


# COMMAND ----------

display(fact_account_metrics)

# COMMAND ----------

fact_account_metrics.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/fact_account_metrics")
