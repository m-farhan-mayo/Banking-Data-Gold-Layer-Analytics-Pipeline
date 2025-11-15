# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

silver_path = "abfss://silver@storage4fintechsd.dfs.core.windows.net/branch/"
df_branch = spark.read.format("parquet").load(silver_path)

# COMMAND ----------

from pyspark.sql import functions as F

# Safely parse 'opened_on' to date
df_branch_clean = df_branch.withColumn(
    "opened_on_clean",
    F.expr("try_to_date(opened_on, 'yyyy-MM-dd')")
)

# Standardize country column
df_branch_clean = df_branch_clean.withColumn(
    "country_region",
    F.when(F.col("country").isNotNull(), F.col("country")).otherwise("Unknown")
)


# COMMAND ----------

dim_branch = df_branch_clean.select(
    "branch_id",
    "branch_name",
    "address",
    "city",
    "country_region",
    "opened_on_clean"
)


# COMMAND ----------

display(df_branch)

# COMMAND ----------

dim_branch.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/dim_branch")
