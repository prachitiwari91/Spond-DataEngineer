# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
import logging

# COMMAND ----------

env = "dev"
first_load = True
common_path = "dbfs:/spond/data/"
db = "spond_raw"


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
        T.StructField(
            "GROUP_ID", T.StringType(), True
        ),  # FK : teams.team_id(team_id do not exist in schema)
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


def cast_to_schema(df, schema):
    for field in schema.fields:
        df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df


def is_schema_valid(df, expected_schema): # todo : enhance the logic
    return df.schema == expected_schema


def read_raw_data(file_name):
    """
    Reads a CSV file from the specified path with header and schema inference options enabled.
    """
    try:
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{common_path}{file_name}")
        )
    except Exception as e:
        logger.error(f"Error reading raw data from file {file_name}: {e}")
        raise


def write_to_delta(df, table_name, partition_column=None):
    """
    Writes a DataFrame to a Delta table with optional partitioning.

    Parameters:
    df (DataFrame): The Spark DataFrame to write.
    table_name (str): The name of the Delta table.
    partition_column (str, optional): The column to partition by. Defaults to None.

    Returns:
    None
    """
    if partition_column:
        df.write.format("delta").mode("overwrite").partitionBy(
            partition_column
        ).saveAsTable(f"{db}.{table_name}")
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.{table_name}")


# Define the function to load data with schema validation
def load_data(file_name, date_column, schema, table_name):
    df = read_raw_data(file_name)
    df = cast_to_schema(df, schema)

    if is_schema_valid(df, schema):
        df = df.withColumn("p_date", F.to_date(date_column))
        write_to_delta(df, table_name, "p_date")
        print(f"Data loaded to {db}{table_name}")
    else:
        print(f"Schema mismatched for {table_name}")


load_data("teams.csv", "CREATED_AT", teams_schema, "raw_teams")
load_data("memberships.csv", "JOINED_AT", memberships_schema, "raw_membership")
load_data("events.csv", "CREATED_AT", events_schema, "raw_events")
load_data("event_rsvps.csv", "RESPONDED_AT", event_rsvps_schema, "raw_events_rsvps")

# COMMAND ----------

df_geo = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{common_path}geo.csv")
)
write_to_delta(df_geo, "d_geo_looker")

# COMMAND ----------

# Validations

# MAGIC %sql
# MAGIC select * from spond_raw.raw_events_rsvps limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_raw.raw_events limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_raw.raw_membership limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spond_raw.raw_teams limit 5;

# COMMAND ----------
