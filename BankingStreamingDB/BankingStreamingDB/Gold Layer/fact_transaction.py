# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

silver_path = "abfss://silver@storage4fintechsd.dfs.core.windows.net/transaction/"
df_transaction = spark.read.format("parquet").load(silver_path)

# COMMAND ----------

# Convert Transaction_Date to timestamp safely
df_trans_clean = df_transaction.withColumn(
    "transaction_ts",
    F.to_timestamp("Transaction_Date")
)

# Standardize columns: trim strings
for c in ["Transaction_ID", "Account_ID", "Transaction_Type", "Currency", "Description"]:
    df_trans_clean = df_trans_clean.withColumn(c, F.trim(F.col(c)))

# COMMAND ----------

# Amount by Account_ID
amount_per_account = df_trans_clean.groupBy("Account_ID").agg(
    F.sum("Amount").alias("total_amount"),
    F.avg("Amount").alias("avg_amount"),
    F.count("*").alias("num_transactions")
)

# Amount by Transaction_Type
amount_per_type = df_trans_clean.groupBy("Transaction_Type").agg(
    F.sum("Amount").alias("total_amount_by_type"),
    F.avg("Amount").alias("avg_amount_by_type"),
    F.count("*").alias("num_transactions_by_type")
)

# Amount by Currency
amount_per_currency = df_trans_clean.groupBy("Currency").agg(
    F.sum("Amount").alias("total_amount_by_currency"),
    F.avg("Amount").alias("avg_amount_by_currency"),
    F.count("*").alias("num_transactions_by_currency")
)

# Daily transaction metrics
daily_transactions = df_trans_clean.groupBy(F.to_date("transaction_ts").alias("transaction_date")).agg(
    F.sum("Amount").alias("total_amount_per_day"),
    F.count("*").alias("transactions_per_day")
)


# COMMAND ----------

display(amount_per_account)
display(amount_per_type)
display(amount_per_currency)

# COMMAND ----------

amount_per_account.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/fact_transaction_account")
amount_per_type.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/fact_transaction_type")
amount_per_currency.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/fact_transaction_currency")
daily_transactions.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/fact_transaction_daily")