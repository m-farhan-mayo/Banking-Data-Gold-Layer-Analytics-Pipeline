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

dim_account_trans_type = df_trans_clean.select(
    "Account_ID",
    "Transaction_Type",
    "Currency"
).dropDuplicates()


# COMMAND ----------

dim_transaction = df_trans_clean.select(
    "Transaction_ID",
    "Account_ID",
    "Transaction_Type",
    "Currency",
    "Description",
    "transaction_ts"
)


# COMMAND ----------

display(dim_account_trans_type.limit(10))
display(dim_transaction.limit(10))

# COMMAND ----------

dim_account_trans_type.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/dim_account_transaction_type")
dim_transaction.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/dim_transaction")