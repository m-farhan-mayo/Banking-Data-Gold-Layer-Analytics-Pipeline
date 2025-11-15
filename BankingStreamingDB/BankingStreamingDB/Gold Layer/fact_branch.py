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

# Add derived column: branch age in days
df_branch_fact = df_branch_clean.withColumn(
    "branch_age_days",
    F.datediff(F.current_date(), "opened_on_clean")
)

# Total branches per country
branches_per_country = df_branch_fact.groupBy("country_region").agg(
    F.count("*").alias("total_branches")
)

# Average branch age per country
avg_branch_age = df_branch_fact.groupBy("country_region").agg(
    F.avg("branch_age_days").alias("avg_branch_age_days")
)

# Total branches per city
branches_per_city = df_branch_fact.groupBy("city").agg(
    F.count("*").alias("branches_in_city")
)


# COMMAND ----------

# Join country-level metrics
fact_branch_country = branches_per_country.join(
    avg_branch_age, on="country_region", how="left"
)

# Optionally, keep city-level metrics separately or join on branch-city
fact_branch_city = branches_per_city


# COMMAND ----------

display(fact_branch_country)
display(fact_branch_city)

# COMMAND ----------

fact_branch_country.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/fact_branch_country")
fact_branch_city.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/fact_branch_city")