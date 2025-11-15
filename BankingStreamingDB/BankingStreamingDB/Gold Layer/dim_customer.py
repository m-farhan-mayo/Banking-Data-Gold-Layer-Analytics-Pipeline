# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

silver_path = "abfss://silver@storage4fintechsd.dfs.core.windows.net/customer/"
df_cust = spark.read.format("parquet").load(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC `1️⃣ DEDUPLICATE (KEEP LATEST RECORD PER CUSTOMER)`

# COMMAND ----------

w = Window.partitionBy("Customer_ID").orderBy(F.col("Created_At").desc())

df_cust = (df_cust.withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn"))

# COMMAND ----------

# MAGIC %md
# MAGIC `2️⃣ STANDARDIZE PHONE (KEEP AS STRING)`

# COMMAND ----------

df_cust = df_cust.withColumn("Phone_Number",F.trim(F.col("Phone").cast("string")))


# COMMAND ----------

# MAGIC %md
# MAGIC `3️⃣ SPLIT ADDRESS INTO LINES`

# COMMAND ----------

df_cust = (df_cust.withColumn("Address_Line1", F.split("Address", "\n").getItem(0))
                          .withColumn("Address_Line2", F.split("Address", "\n").getItem(1))
                          .drop("Address")
                          .drop("Phone")
                          .drop("Created_At"))

# COMMAND ----------

# MAGIC %md
# MAGIC `4️⃣ EXTRACT STATE + ZIP FROM ADDRESS_LINE2`

# COMMAND ----------

df_cust = (df_cust.withColumn("State",F.regexp_extract(F.col("Address_Line2"), r", ([A-Z]{2}) ", 1))
                      .withColumn("Postal_Code",F.regexp_extract(F.col("Address_Line2"), r"([0-9]{5})$", 1)))

# COMMAND ----------

# MAGIC %md
# MAGIC `5️⃣ ADD GOLD AUDIT COLUMNS`

# COMMAND ----------

df_cust = (df_cust.withColumn("record_created_date", F.current_timestamp())
                           .withColumn("is_active", F.lit(True))
                           .withColumn("source_system", F.lit("core_banking_system")))

# COMMAND ----------

display(df_cust.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC `7️⃣ WRITE TO GOLD LAYER (DELTA)`

# COMMAND ----------

df_cust.write.format("delta").mode("overwrite").save("abfss://gold@storage4fintechsd.dfs.core.windows.net/dim_customer/")

print("Gold Layer: dim_customer created successfully!")
