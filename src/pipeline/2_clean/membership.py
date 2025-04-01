# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

env= 'dev'
src_db='spond_raw'
tgt_db='spond_clean'
partition_start_date = '2024-01-01'
partition_end_date = '2025-03-30'

# COMMAND ----------

def join_team_table(df):
    df_team = (
        spark.table(
            f"{tgt_db}.clean_teams"
        )
        .filter(F.col("is_valid_record"))
        .select(
            F.col("team_id").alias("team_id_ref"),
        ).distinct()
    )

    return df.join(
        df_team, F.col("team_id") == F.col("team_id_ref"), "left"
    )
  
def add_validation_flags(df):
    return (
        df.withColumn("is_valid_team_id", F.col("team_id_ref").isNotNull())
          .withColumn("is_valid_record", F.col("is_valid_team_id"))
          .drop("team_id_ref")
    )

def add_validation_error_columns(df):
    return (
        df.withColumn(
            "validation_error", 
            F.when(
                ~F.col("is_valid_record"), 
                F.concat_ws(", ",
                    F.when(~F.col("is_valid_team_id"), F.lit("Invalid FK team_id"))
                )
            ).otherwise(F.lit(None))
        )
    )

def select_input_columns(df):
  return df.select("membership_id", F.col("group_id").alias("team_id"), "role_title", "joined_at", "p_date")

# COMMAND ----------

transformed_df = (
  spark.table(f"{src_db}.raw_membership")
    .filter(F.col("p_date").between(partition_start_date, partition_end_date))
    .filter(F.col("membership_id").isNotNull())
    .dropDuplicates(["membership_id"])
    .transform(select_input_columns)
    .transform(join_team_table)
    .transform(add_validation_flags)
    .transform(add_validation_error_columns)
)

# COMMAND ----------

#Write data to delta tables
(
  transformed_df.write
    .mode("overwrite")
    .format("delta")
    .partitionBy("p_date")
    .saveAsTable(f"{tgt_db}.clean_membership")
)

# COMMAND ----------

# Test if the data was written correctly

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),'raw' as table_name from spond_raw.raw_membership
# MAGIC union
# MAGIC select count(*),'clean' as table_name from spond_clean.clean_membership
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_clean.clean_membership where is_valid_record=false