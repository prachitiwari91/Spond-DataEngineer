
# TODO:
# Create an Airflow DAG for loading the Spond data.
# raw-->clean-->fact/dim-->fact_enriched
# Dependencies:
# 1. load_raw_data
# 2. load_raw_data >> load_clean_teams_data >> load_clean_membership_data >> load_clean_events_data
# 3. load_clean_events_rsvps_data >> load_geo_looker_data
# 4. load_dim_teams_data
# 5. load_dim_membership_data
# 6. load_dim_events_data
# 7. load_dim_geo_looker_data
# 8. load_fact_rsvp_data
# 9. (
#       [
#           load_dim_teams_data,
#           load_dim_membership_data,
#           load_dim_events_data,
#           load_dim_geo_looker_data,
#           load_fact_rsvp_data,
#       ]
#       >> load_f_events_enriched
#    )