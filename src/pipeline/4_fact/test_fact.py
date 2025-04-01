# Databricks notebook source
# Replace with your table or view
tgt_db = 'spond_analytics'
FACT_TABLE = f"{tgt_db}.v_f_events_rsvps"

def test_fact_view_accessible():
    df = spark.sql(f"SELECT * FROM {FACT_TABLE} LIMIT 1")
    assert df.count() >= 0, f"{FACT_TABLE} is not accessible"
    print("test_fact_view_accessible passed")

def test_required_columns_present():
    df = spark.sql(f"SELECT * FROM {FACT_TABLE} LIMIT 1")
    expected_cols = {
        "event_rsvp_id", "event_id", "membership_id", "rsvp_status", "responded_at",
        "event_start", "event_end", "event_duration_minutes",
        "team_id", "country_code", "role_title",
        "etl_created_date","p_date"
    }
    missing = expected_cols - set(df.columns)
    assert not missing, f"Missing columns: {missing}"
    print("test_required_columns_present passed")

def test_pk_not_null():
    df = spark.sql(f"SELECT * FROM {FACT_TABLE} WHERE event_rsvp_id IS NULL")
    assert df.count() == 0, "event_rsvp_id has null values"
    print("test_pk_not_null passed")

def test_valid_rsvp_status():
    df = spark.sql(f"SELECT * FROM {FACT_TABLE} WHERE rsvp_status NOT IN (0, 1, 2)")
    assert df.count() == 0, " Invalid rsvp_status values found"
    print("test_valid_rsvp_status passed")

def test_event_dates_not_null():
    df = spark.sql(f"SELECT * FROM {FACT_TABLE} WHERE event_start IS NULL OR event_end IS NULL")
    assert df.count() == 0, "Nulls in event_start or event_end"
    print("test_event_dates_not_null passed")

def test_etl_columns_exist():
    df = spark.sql(f"SELECT * FROM {FACT_TABLE} WHERE etl_created_date IS NULL")
    assert df.count() == 0, "etl_created_date has null"
    print("test_etl_columns_exist passed")

# COMMAND ----------

test_fact_view_accessible()
test_required_columns_present()
test_pk_not_null()
test_valid_rsvp_status()
test_event_dates_not_null()
test_etl_columns_exist()

# COMMAND ----------

#ToDo : Similary more test to be added for other tables