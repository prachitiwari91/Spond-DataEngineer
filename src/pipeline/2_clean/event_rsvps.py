# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# TODO : Define as Databricks job parameters
env= 'dev'
src_db='spond_raw'
tgt_db='spond_clean'
partition_start_date = '2024-01-01'
partition_end_date = '2025-03-30'
first_load=True

# COMMAND ----------

rsvp_status =[0,1,2]

# COMMAND ----------

def join_membership_table(df):
    df_membership = (
        spark.table(
            f"{tgt_db}.clean_membership"
        )
        .filter(F.col("is_valid_record"))
        .select(
            F.col("membership_id").alias("membership_id_ref"),
        ).distinct()
    )

    return df.join(
        df_membership, F.col("membership_id") == F.col("membership_id_ref"), "left"
    )

def join_events_table(df):
    df_events = (
        spark.table(
            f"{tgt_db}.clean_events"
        )
        .filter(F.col("is_valid_record"))
        .select(
            F.col("event_id").alias("event_id_ref"),
        ).distinct()
    )

    return df.join(
        df_events, F.col("event_id") == F.col("event_id_ref"), "left"
    )

def add_validation_flags(df):
    return (
        df.withColumn("is_valid_event_id", F.col("event_id_ref").isNotNull())
          .withColumn("is_valid_membership_id", F.col("membership_id_ref").isNotNull())
          .withColumn("is_valid_rsvp", F.col("rsvp_status").isin(rsvp_status))
          .withColumn("is_valid_record", F.col("is_valid_event_id") & F.col("is_valid_membership_id") & F.col("is_valid_rsvp"))
          .drop("event_id_ref", "membership_id_ref")
    )

def add_validation_error_columns(df):
    return (
        df.withColumn(
            "validation_error", 
            F.when(
                ~F.col("is_valid_record"), 
                F.concat_ws(", ",
                    F.when(~F.col("is_valid_event_id"), F.lit("Invalid FK event_id")),
                    F.when(~F.col("is_valid_membership_id"), F.lit("Invalid FK membership_id")),
                    F.when(~F.col("is_valid_rsvp"), F.lit("Invalid rsvp status"))
                )
            ).otherwise(F.lit(None))
        )
    )

def add_metadata_columns(df):
    return (
        df.withColumn("etl_created_date", F.current_timestamp())
    )

def add_enriched_columns(df):
    return (
        df.withColumn(
            "rsvp_response", 
            F.when(F.col("rsvp_status") == 0, F.lit("unanswered"))
             .when(F.col("rsvp_status") == 1, F.lit("accepted"))
             .when(F.col("rsvp_status") == 2, F.lit("declined"))
             .otherwise(F.lit("n/a"))
        )
    )

def add_latest_response_flag(df):
    rsvp_window = Window.partitionBy("event_id", "membership_id").orderBy(F.col("responded_at").desc())
    return (
        df.withColumn("row_num", F.row_number().over(rsvp_window))
    .withColumn("is_latest_response", F.when(F.col("row_num") == 1, F.lit(True)).otherwise(F.lit(False)))
    .drop("row_num")
    )


# COMMAND ----------

transformed_df = (
    spark.table(f"{src_db}.raw_events_rsvps")
    .filter(F.col("p_date").between(partition_start_date, partition_end_date))
    .filter(F.col("event_rsvp_id").isNotNull())
    .dropDuplicates(["event_rsvp_id"])
    .transform(join_membership_table)
    .transform(join_events_table)
    .transform(add_enriched_columns)
    .transform(add_latest_response_flag)
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
    .saveAsTable(f"{tgt_db}.clean_events_rsvps")
)

# COMMAND ----------

# Validation
# TODO: Add unit test for the tables in test folder

# COMMAND ----------

# MAGIC %sql
# MAGIC --Check schema is as expected
# MAGIC select * from spond_clean.clean_events_rsvps limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- total count should same
# MAGIC select count(*), "raw" as table_name from spond_clean.raw_events_rsvps group by all
# MAGIC union
# MAGIC select count(*), "clean" as table_name from spond_clean.clean_events_rsvps group by all

# COMMAND ----------

# MAGIC %sql
# MAGIC --chcek the is_latest response is populated correctly:
# MAGIC select * from spond_clean.clean_events_rsvps where event_id='C7362D86365C471C94EAA1107314029A' AND membership_id='6371824233117a5dff776767a8cec3da147be688e9edf9a7a0ff0011de985705' group by all 

# COMMAND ----------

# MAGIC %sql
# MAGIC select is_latest_response, count(*),count(distinct event_rsvp_id) from spond_clean.clean_events_rsvps where is_valid_record=true group by all

# COMMAND ----------

# MAGIC %sql
# MAGIC -- is_valid_record should be populated correctly
# MAGIC select is_valid_record, count(*),count(distinct event_rsvp_id) from spond_clean.clean_events_rsvps group by all

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run the implicit join with source table and check if all records in tgt are valid
# MAGIC select count(*) from spond_clean.clean_events_rsvps where validation_error like '%event_id%' group by all

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from spond_clean.clean_events_rsvps where validation_error like '%membership_id%' group by all

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  validation_error, count(distinct event_rsvp_id) FROM spond_clean.error_events_rsvps GROUP BY all

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from spond_clean.raw_events_rsvps  r
# MAGIC left join spond_clean.clean_membership m on r.MEMBERSHIP_ID=m.membership_id and is_valid_record=true
# MAGIC where m.membership_id is null 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from spond_clean.raw_events_rsvps  r
# MAGIC left join spond_clean.clean_events m on r.event_ID=m.EVENT_ID and is_valid_record=true
# MAGIC where m.EVENT_ID is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from spond_clean.raw_events_rsvps join spond_clean.clean_events using(event_id)
# MAGIC join spond_clean.clean_membership using(membership_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNTRY_CODE,state,LATITUDE,min_lat,max_lat,LONGITUDE,min_lng,max_lng,count(*) from spond_clean.raw_events 
# MAGIC join spond_clean.raw_teams using(team_id)  
# MAGIC left join spond_clean.geo_looker on country_code=countryCode and LATITUDE between min_lat and max_lat and LONGITUDE between min_lng and max_lng group by COUNTRY_CODE,state,LATITUDE,min_lat,max_lat,LONGITUDE,min_lng,max_lng