# Databricks notebook source
tgt_db='spond_analytics'

# COMMAND ----------

#  Test 1: Check view exists and is queryable
def test_view_accessible():
    df = spark.sql(f"SELECT * FROM {tgt_db}.v_d_teams LIMIT 1")
    assert df.count() >= 0  # Will fail only if view is broken

# 🔍 Test 2: Check expected columns in the view
def test_view_schema():
    df = spark.sql(f"SELECT * FROM {tgt_db}.v_d_teams")
    expected_columns = {
        "team_id", "team_activity", "country_code", "created_at",
        "etl_created_date", "etl_updated_date"
    }
    assert expected_columns.issubset(set(df.columns))

# 🔍 Test 3: No NULLs in primary key column
def test_team_id_not_null():
    df = spark.sql(f"SELECT * FROM {tgt_db}.v_d_teams WHERE team_id IS NULL")
    assert df.count() == 0

test_view_accessible()
test_view_schema()
test_team_id_not_null()