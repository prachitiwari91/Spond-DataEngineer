# Databricks notebook source
from pyspark.sql import functions as F, types as T

# COMMAND ----------

# TODO : Define as Databricks job parameters
env= 'dev'
src_db='spond_clean'
tgt_db='spond_analytics'
partition_start_date = '2024-01-01'
partition_end_date = '2025-03-30'
full_load=True

# COMMAND ----------

def select_input_columns_dim_teams(df):
  return (
    df.select("team_id", "team_activity", "country_code", "created_at")
  )
  
def select_input_columns_dim_membership(df):
    return df.select("membership_id", "team_id", "role_title", "joined_at")

def select_input_columns_dim_event(df):
    return df.select("event_id", "team_id", "event_start", "event_end", "event_duration_minutes", "has_location", "latitude", "longitude", "created_at")
  
def add_metadata(df):
    return (
        df.withColumn("etl_created_date", F.current_timestamp())
          .withColumn("etl_updated_date", F.current_timestamp())
    )

def run_etl(
    src_table: str,
    tgt_table: str,
    input_selector: callable,
    merge_condition: str,
    insert_columns: list,
    full_load: bool
):
    # Load and transform
    df = (
        spark.read.table(src_table)
        .filter(F.col("p_date").between(partition_start_date, partition_end_date))
        .filter(F.col("is_valid_record") == True)
        .transform(input_selector)
        .transform(add_metadata)
    )

    df.createOrReplaceTempView("temp_etl_view")

    if full_load:
        df.write.mode("overwrite").format("delta").saveAsTable(tgt_table)
    else:
        columns_str = ", ".join(insert_columns)
        values_str = ", ".join([f"source.{col}" for col in insert_columns])

        merge_query = f"""
        MERGE INTO {tgt_table} AS target
        USING temp_etl_view AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET
            {", ".join([f"{col} = source.{col}" for col in insert_columns if col != "etl_created_date"])}
        WHEN NOT MATCHED THEN INSERT ({columns_str})
        VALUES ({values_str})
        """

        spark.sql(merge_query)

def create_view(view_schema: str, view_name: str, source_table: str):
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_schema}.{view_name} AS
        SELECT * FROM {source_table}
    """)
    print(f"View created: {view_schema}.{view_name} from {source_table}")


# COMMAND ----------

# Dimension: Teams
src_table="clean_teams"
tgt_table="d_teams"
run_etl(
    src_table=f"{src_db}.{src_table}",
    tgt_table=f"{tgt_db}.{tgt_table}",
    input_selector=select_input_columns_dim_teams,
    merge_condition="target.team_id = source.team_id",
    insert_columns=[
       "team_id", "team_activity", "country_code", "created_at", "etl_created_date", "etl_updated_date"
    ],
    full_load=full_load
)

create_view(view_schema=tgt_db, view_name=f"v_{tgt_table}", source_table=f"{tgt_db}.{tgt_table}")

# COMMAND ----------

# Dimension: Membership
src_table="clean_membership"
tgt_table="d_membership"
run_etl(
    src_table=f"{src_db}.{src_table}",
    tgt_table=f"{tgt_db}.{tgt_table}",
    input_selector=select_input_columns_dim_membership,
    merge_condition="target.membership_id = source.membership_id",
    insert_columns=[
        "membership_id", "team_id", "role_title", "joined_at", "etl_created_date", "etl_updated_date"
    ],
    full_load=full_load
)

create_view(view_schema=tgt_db, view_name=f"v_{tgt_table}", source_table=f"{tgt_db}.{tgt_table}")

# COMMAND ----------

# Dimension: Events
src_table="clean_events"
tgt_table="d_events"
run_etl(
    src_table=f"{src_db}.{src_table}",
    tgt_table=f"{tgt_db}.{tgt_table}",
    input_selector=select_input_columns_dim_event,
    merge_condition="target.event_id = source.event_id",
    insert_columns=[
        "event_id", "team_id", "event_start", "event_end", "event_duration_minutes",
        "has_location", "latitude", "longitude", "created_at", "etl_created_date", "etl_updated_date"
    ],
    full_load=full_load
)

create_view(view_schema=tgt_db, view_name=f"v_{tgt_table}", source_table=f"{tgt_db}.{tgt_table}")

# COMMAND ----------

#Dimension: Geo_looker
tgt_table= "d_geo_looker"
create_view(view_schema=tgt_db, view_name=f"v_{tgt_table}", source_table=f"{src_db}.{tgt_table}")

# COMMAND ----------

# Validations
# TODO: write the validation as part of unit test into separate notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),'view'  as table_name from spond_clean.v_d_teams
# MAGIC union 
# MAGIC select count(*), 'clean' as table_name from spond_analytics.clean_teams where is_valid_record=true

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),'view'  as table_name from spond_clean.v_d_membership
# MAGIC union 
# MAGIC select count(*), 'clean' as table_name from spond_analytics.clean_membership where is_valid_record=true
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),'view'  as table_name from spond_clean.v_d_events
# MAGIC union 
# MAGIC select count(*), 'clean' as table_name from spond_analytics.clean_events where is_valid_record=true

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_analytics.v_d_teams where created_at::date='2024-03-30' limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_analytics.v_d_membership where joined_at::date='2024-03-30' limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_analytics.v_d_events where created_at::date='2024-03-30' limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_analytics.d_geo_looker