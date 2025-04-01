# Databricks notebook source
from pyspark.sql import functions as F, types as T

# COMMAND ----------

# TODO : Define as Databricks job parameters
env= 'dev'
src_db='spond_analytics'
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

fact_event_enriched_df = spark.sql(f"""
WITH rsvp_summary AS (
  SELECT
     event_id,
     COUNT(*) AS total_rsvps,
     COUNT(CASE WHEN rsvp_status = 1 THEN 1 END) AS accepted_rsvps,
     COUNT(CASE WHEN rsvp_status = 2 THEN 1 END) AS declined_rsvps,
     COUNT(CASE WHEN rsvp_status = 0 THEN 1 END) AS unanswered_rsvps,
     COUNT(CASE WHEN rsvp_status IN (1, 2) THEN 1 END) AS responded_members
  FROM {tgt_db}.f_events_rsvps
  WHERE is_latest_response = TRUE
  GROUP BY event_id
),

geo_match AS (
  SELECT
    e.event_id,
    g.state AS region,
    ROW_NUMBER() OVER (
      PARTITION BY e.event_id
      ORDER BY g.state -- or some other criteria
    ) AS rn
  FROM {tgt_db}.d_events e
  JOIN {tgt_db}.d_teams t ON e.team_id = t.team_id
  LEFT JOIN {tgt_db}.d_geo_looker g
    ON e.has_location = TRUE
     AND g.country_code = t.country_code
     AND e.latitude BETWEEN g.min_lat AND g.max_lat
     AND e.longitude BETWEEN g.min_lng AND g.max_lng
),

geo_deduped AS (
  SELECT event_id, region
  FROM geo_match
  WHERE rn = 1
)

SELECT
  e.event_id,
  e.team_id,
  e.event_start,
  e.event_end,
  DATE(e.event_start) AS event_date,
  e.event_duration_minutes,

  t.team_activity,
  t.country_code,
  g.region,

  r.total_rsvps,
  r.accepted_rsvps,
  r.declined_rsvps,
  r.unanswered_rsvps,
  r.responded_members,

  ROUND(
    CASE 
      WHEN r.total_rsvps > 0 THEN 100.0 * r.accepted_rsvps / r.total_rsvps
      ELSE 0
    END, 2
  ) AS event_attendance_rate

FROM {tgt_db}.d_events e
LEFT JOIN {tgt_db}.d_teams t ON e.team_id = t.team_id
LEFT JOIN geo_deduped g ON e.event_id = g.event_id
LEFT JOIN rsvp_summary r ON e.event_id = r.event_id
""")

(fact_event_enriched_df.transform(add_metadata).write 
    .mode("overwrite") 
    .format("delta")
    .partitionBy("event_date")
    .saveAsTable(f"{tgt_db}.f_events_enriched"))

tgt_table = 'f_events_enriched'
create_view(view_schema=tgt_db, view_name=f"v_{tgt_table}", source_table=f"{tgt_db}.{tgt_table}")


# COMMAND ----------

# TODO: Create `fact_membership_enriched` to track daily member engagement.
# This table will help answer:
#   1. What is the daily active membership count?
#   2. How many new members joined on a given day?
#   3. How many returning members were active on each day?
#   5. further can be enehaced to weekly, monthly yearly aggregates to understand the lifecycle of members.


# COMMAND ----------

# MAGIC %sql
# MAGIC --Daily active teams: d_events or f_events_enriched
# MAGIC --RSVP summary: f_events_enriched
# MAGIC --Attendance Rate (Last 30 Days): f_events_rsvp
# MAGIC --Events hosted per region f_events_enriched
# MAGIC --New vs. returning members: How many new members joined each week, and how many were returning (already joined in a previous week)?
# MAGIC --joined_week= respond_at_week -->new member
# MAGIC --joined_week = respond_at_week --->returning member

# COMMAND ----------

# Validations
# TODO: write the validation as part of unit test into separate notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), 'view' as table_name from spond_analytics.v_f_events_enriched
# MAGIC union 
# MAGIC select count(*), 'clean' as table_name from spond_analytics.d_events

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_analytics.f_events_enriched limit 5