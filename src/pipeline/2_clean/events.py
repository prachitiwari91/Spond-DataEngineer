# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# TO DO : Define as job parameter
env= 'dev'
src_db='spond_raw'
tgt_db='spond_clean'
partition_start_date = '2024-01-01'
partition_end_date = '2025-03-30'
first_load=True

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
          .withColumn("is_valid_timestamps", F.col("event_start") <= F.col("event_end"))
          .withColumn("is_valid_record", F.col("is_valid_team_id") & F.col("is_valid_timestamps"))
          .drop("team_id_ref")
    )

def add_validation_error_columns(df):
    return (
        df.withColumn(
            "validation_error", 
            F.when(
                ~F.col("is_valid_record"), 
                F.concat_ws(", ",
                    F.when(~F.col("is_valid_team_id"), F.lit("Invalid FK team_id")),
                    F.when(~F.col("is_valid_timestamps"), F.lit("Invalid Timestamps"))
                )
            ).otherwise(F.lit(None))
        )
    )

def add_enriched_columns(df):
  return (
    df.withColumn("has_location", F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
      .withColumn("event_duration_minutes", (F.col("event_end").cast("long") - F.col("event_start").cast("long")) / 60)
  )

# COMMAND ----------

transformed_df = (
    spark.table(f"{src_db}.raw_events")
    .filter(F.col("p_date").between(partition_start_date, partition_end_date))
    .filter(F.col("event_id").isNotNull())
    .dropDuplicates(["event_id"])
    .transform(join_team_table)
    .transform(add_validation_flags)
    .transform(add_enriched_columns)
    .transform(add_validation_error_columns)
)

# COMMAND ----------

#Write data to delta tables
(
  transformed_df.write
    .mode("overwrite")
    .format("delta")
    .partitionBy("p_date")
    .saveAsTable(f"{tgt_db}.clean_events")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_clean.clean_events limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),'raw' as table_name from spond_raw.raw_events group by all
# MAGIC union
# MAGIC select count(*),'clean' as table_name from spond_clean.clean_events group by all
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_clean.clean_events where is_valid_record = false