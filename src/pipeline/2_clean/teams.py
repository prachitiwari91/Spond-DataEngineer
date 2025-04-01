# Databricks notebook source
from pyspark.sql import functions as F, types as T

# COMMAND ----------

env= 'dev'
src_db='spond_raw'
tgt_db='spond_clean'
partition_start_date = '2024-01-01' # Yesterday_date()
partition_end_date = '2025-03-30' # partition_start_date

#for the first load get the min and max p_date from raw table

# COMMAND ----------

# ISO country codes allowed (extend as needed)
valid_country_codes = ["NOR", "GBR", "USA", "DEU"]

# COMMAND ----------

def add_validation_flags(df):
    return (
        df.withColumn("is_valid_country_code", F.col("country_code").isin(valid_country_codes))
          .withColumn("is_valid_record", F.col("is_valid_country_code"))
    )

def add_validation_error_columns(df):
  return (
        df.withColumn("validation_error",
        F.when(~F.col("is_valid_country_code"), F.lit("Invalid country_code"))
    )
    )


# COMMAND ----------

transformed_df = (
    spark.read.table(f"{src_db}.raw_teams")
    .filter(F.col("p_date").between(partition_start_date, partition_end_date))
    .filter(F.col("team_id").isNotNull())
    .dropDuplicates(["team_id"])
    .transform(add_validation_flags)
    .transform(add_validation_error_columns)
)

# COMMAND ----------

# Write data to delta tables
(
  transformed_df.write
    .mode("overwrite")
    .format("delta")
    .partitionBy("p_date")
    .saveAsTable(f"{tgt_db}.clean_teams")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_clean.clean_teams limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Validation
# MAGIC select count(*), "raw" as table_name from spond_raw.raw_teams group by all
# MAGIC union
# MAGIC select count(*), "clean" as table_name from spond_clean.clean_teams group by all