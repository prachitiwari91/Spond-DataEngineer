# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Local Spark session initialization
spark = SparkSession.builder \
    .appName("SpondRawETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Helper: Yesterday's date (used in scheduling)
def get_yesterday_str():
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# Set these manually when running locally
# ToDo: Create databricks job level paramters to be set from the job/dag
env = "dev"
src_db = "src_db"
tgt_db = "spond_raw"
first_load = True  # or False, depending on your use case
partition_start_date = "2024-01-01"  # Example date, replace with actual date or yesterday_str for scheduling the daily pipelines
partition_end_date = "2025-03-30"  # Example date, replace with partition_start_date for single day or actual date


# COMMAND ----------

# Teams table
teams_schema = T.StructType(
    [
        T.StructField("TEAM_ID", T.StringType(), True),  # PK
        T.StructField("GROUP_ACTIVITY", T.StringType(), True),
        T.StructField("COUNTRY_CODE", T.StringType(), True),
        T.StructField("CREATED_AT", T.TimestampType(), True),  # UTC timestamp
    ]
)

# Memberships table
memberships_schema = T.StructType(
    [
        T.StructField("MEMBERSHIP_ID", T.StringType(), True),  # PK
        T.StructField("GROUP_ID", T.StringType(), True),  # FK : teams.team_id
        T.StructField("ROLE_TITLE", T.StringType(), True),
        T.StructField("JOINED_AT", T.TimestampType(), True),  # UTC timestamp
    ]
)

# Events table
events_schema = T.StructType(
    [
        T.StructField("EVENT_ID", T.StringType(), True),  # PK
        T.StructField("TEAM_ID", T.StringType(), True),  # FK :teams.team_id
        T.StructField("EVENT_START", T.TimestampType(), True),  # UTC timestamp
        T.StructField("EVENT_END", T.TimestampType(), True),  # UTC timestamp
        T.StructField("LATITUDE", T.DoubleType(), True),
        T.StructField("LONGITUDE", T.DoubleType(), True),
        T.StructField("CREATED_AT", T.TimestampType(), True),  # UTC timestamp
    ]
)

# Event RSVPs table
event_rsvps_schema = T.StructType(
    [
        T.StructField("EVENT_RSVP_ID", T.StringType(), True),  # PK
        T.StructField("EVENT_ID", T.StringType(), True),  # FK : events.event_id
        T.StructField(
            "MEMBERSHIP_ID", T.StringType(), True
        ),  # FK : memberships.membership_id
        T.StructField("RSVP_STATUS", T.IntegerType(), True),  # Enum: 0, 1, 2
        T.StructField("RESPONDED_AT", T.TimestampType(), True),  # UTC timestamp
    ]
)


# COMMAND ----------


def cast_to_schema(df, schema):
    for field in schema.fields:
        df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df


def is_schema_valid(df, expected_schema):
    return df.schema == expected_schema


# COMMAND ----------

jdbc_url = f"jdbc:postgresql://{dbutils.secrets.get('jdbc', 'postgres-host')}:5432/{dbutils.secrets.get('jdbc', 'postgres-db')}"

connection_properties = {
    "user": dbutils.secrets.get("jdbc", "postgres-user"),
    "password": dbutils.secrets.get("jdbc", "postgres-password"),
    "driver": "org.postgresql.Driver",
}


# Define the function to load data with schema validation
def load_data(table_name, date_column, schema):
    columns_str = ", ".join([field.name for field in schema.fields])
    if first_load:
        custom_query = f"""
        SELECT {columns_str}
        FROM {src_db}.{table_name}
        """
    else:
        custom_query = f"""
        SELECT {columns_str}
        FROM {src_db}.{table_name}
        WHERE DATE({date_column}) between '{partition_start_date}' and '{partition_end_date}'
        """

    df = spark.read.jdbc(
        url=jdbc_url, table=custom_query, properties=connection_properties
    )

    df = cast_to_schema(df, schema)

    if is_schema_valid(df, schema):
        df = df.withColumn("p_date", F.to_date(F.col(date_column)))
        df.write.format("delta").mode("overwrite").partitionBy("p_date").saveAsTable(
            f"{tgt_db}.{table_name}"
        )
    else:
        print("Schema mismatched. Please have a look at the schema")


# COMMAND ----------

# Load raw data to teams
load_data("teams", "CREATED_AT", teams_schema)

# COMMAND ----------

# Load raw data to membership
load_data("membership", "JOINED_AT", memberships_schema)

# COMMAND ----------

# Load raw data to events
load_data("events", "CREATED_AT", events_schema)

# COMMAND ----------

# Load raw data to events_rsvps
load_data("events_rsvps", "RESPONDED_AT", event_rsvps_schema)

# COMMAND ----------

# Validations

# MAGIC %sql
# MAGIC select * from spond_raw.raw_teams

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_raw.raw_membership

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_raw.raw_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_raw.raw_events_rsvps;
