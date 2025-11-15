# Databricks notebook source
# MAGIC %md
# MAGIC ## `Branches Silver Transforamtions`

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

bronze_path = "abfss://bronze@storage4fintechsd.dfs.core.windows.net/branches/"
df = spark.read.format("parquet").load(bronze_path)

# COMMAND ----------

branch_schema = StructType([
    StructField("branch_id", StringType(), True),
    StructField("branch_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("opened_on", StringType(), True)
])

# COMMAND ----------

json_df = df.withColumn(
    "branch_json",
    from_json(col("raw_data"), branch_schema)
)

# COMMAND ----------

df_branch = json_df.select(
    col("raw_data"),
    col("branch_json.*")
)


# COMMAND ----------

df_branch = df_branch.drop("raw_data")\
                     .dropna(subset=["branch_id"],how='all')\
                     .dropDuplicates(subset=["branch_id"])\
                     .withColumn("address",regexp_replace(col("address"), ",", "-"))
display(df_branch)

# COMMAND ----------

df_branch.write.mode("overwrite").format("parquet").save("abfss://silver@storage4fintechsd.dfs.core.windows.net/branch/")