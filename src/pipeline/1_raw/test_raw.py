from pyspark.sql import SparkSession

# Set your common path here
common_path = "<path_to_your_csv_files>" # Update this to your actual path

db = 'spond_raw'  # Example, update to your actual path

def validate_csv_vs_table_counts(common_path, file_table_mapping):
    for file_name, table_name in file_table_mapping.items():
        # Read CSV
        df = spark.read.option("header", True).csv(f"{common_path}{file_name}.csv")
        csv_count = df.count()
        
        # Read table count
        table_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]["cnt"]
        
        print(f"Validating: {file_name}.csv vs {table_name}")
        print(f"CSV count     : {csv_count}")
        print(f"Table count   : {table_count}")
        
        # Assertion
        assert csv_count == table_count, f"Mismatch in counts for {file_name}: CSV({csv_count}) != Table({table_count})"
        print("Count matched\n")

# Mapping of CSV file base names to Delta table names
file_table_mapping = {
    "teams": f"{db}.raw_teams",
    "events": f"{db}.raw_events",
    "event_rsvps": f"{db}.raw_events_rsvps",
    "memberships": f"{db}.raw_membership"
}

# Run validation
validate_csv_vs_table_counts(common_path, file_table_mapping)

#To do:
# add more tests