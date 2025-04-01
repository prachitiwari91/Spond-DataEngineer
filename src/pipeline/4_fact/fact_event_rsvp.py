# Databricks notebook source
from pyspark.sql import functions as F, types as T

# COMMAND ----------

# TODO : Define as Databricks job parameters
env= 'dev'
src_db='spond_clean'
tgt_db='spond_analytics'
partition_start_date = '2024-01-01'
partition_end_date = '2025-03-30'

# COMMAND ----------

def add_metadata(df):
    return (
        df.withColumn("etl_created_date", F.current_timestamp())
    )

def create_view(view_schema: str, view_name: str, source_table: str):
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_schema}.{view_name} AS
        SELECT * FROM {source_table}
    """)
    print(f"View created: {view_schema}.{view_name} from {source_table}")

# COMMAND ----------

transformed_df = (
        spark.read.table(f"{src_db}.clean_events_rsvps")
        .filter(F.col("is_valid_record") == True)
        .filter(F.col("p_date").between(partition_start_date, partition_end_date))
        .transform(add_metadata)
        .select(
        "EVENT_RSVP_ID",
        "EVENT_ID",
        "MEMBERSHIP_ID",
        "RSVP_STATUS",
        "RESPONDED_AT",
        "IS_LATEST_RESPONSE",
        "P_DATE",
        "ETL_CREATED_DATE"
    )
    )

# COMMAND ----------

(transformed_df.write 
    .mode("overwrite") 
    .format("delta")
    .partitionBy("p_date")
    .saveAsTable(f"{tgt_db}.f_events_rsvps"))

# COMMAND ----------

tgt_table = 'f_events_rsvps'
create_view(view_schema=tgt_db, view_name=f"v_{tgt_table}", source_table=f"{tgt_db}.{tgt_table}")

# COMMAND ----------

# Validations
# TODO: write the validation as part of unit test into separate notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), 'view' as table_name from spond_analytics.v_f_events_rsvps
# MAGIC union 
# MAGIC select count(*), 'clean' as table_name from spond_analytics.clean_events_rsvps

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from spond_analytics.clean_events_rsvps where is_valid_record = true