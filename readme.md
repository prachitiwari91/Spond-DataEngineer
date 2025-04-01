# Spond Data Engineering Challenge

## Objective

This repository presents a data pipeline solution simulating a data platform for **Spond**, enabling ingestion, validation, transformation, and analytical modeling of data related to teams, events, memberships, and RSVPs.

---

## Project Structure

```
src/
└── pipeline/
    ├── 1_raw/            # Data ingestion from PostgreSQL/CSV into Delta Lake
    ├── 2_clean/          # Data cleansing, validation, enrichment
    ├── 3_dim/            # Dimension table creation and loading
    ├── 4_fact/           # Fact table generation and enrichment
    └── dag/              # DAG orchestration (Airflow design placeholder)
```

---

## How to Run

### Prerequisites

- PySpark and Databricks environment
- Delta Lake enabled
- JDBC config and secrets set up using Databricks secrets
- Mentioned databases already created
- CSV files are kept at the S3 location.

### Step-by-Step

1. **Clone the repository**

2. **Set Job Parameters**
   In each notebook/script, configure:
   ```python
   env = "dev"
   src_db = "src_db"
   tgt_db = "spond_raw"  # or clean/analytics depending on the layer
   ```

3. **Run Notebooks in Order**

   - `1_raw/raw_load.py`
   - `2_clean/{teams.py, membership.py, events.py, event_rsvps.py}`
   - `3_dim/dim_load.py`
   - `4_fact/fact_event_rsvp.py`
   - `5_fact/fact_event_enriched.py`

4. **(Optional) Define Airflow DAG**  
   See `dag/spond_ingestion_pipeline.py` for orchestration planning.

---

## Sample Queries

Example:
```sql
-- Daily Active Team
SELECT
event_start::date as event_date,count(distinct team_id) as active_teams
FROM spond_analytics.v_d_events
group by event_date;
```

---

## Tests

Sample Unit tests are defined in:
- `1_raw/test_raw.py`
- `3_dim/test_dim.py`
- `4_fact/test_fact.py`

(Extend these with `pytest` or `unittest` for local CI.)

---

### Layered Architecture

```
db.table_prefix
spond_raw.raw_tableName
   ↓
spond_clean.clean_tableName
   ↓
spond_analytics.f_tableName / analytics.d_tableName (View--> v_)
   ↓
spond_analytics.f_tableName_enriched (Views-->v_)
   ↓
spond_aggregates.tableName_agg(Views)
```

---

### Database Structure

| Layer         | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `spond_raw`         | Ingested source data (e.g., from S3, databases)                |
| `spond_clean`       | Validated, de-duplicated, and schema-aligned tables                         |
| `spond_analytics`   | Star schema: dimension and fact tables (used for downstream modeling)       |
| `spond_aggregates`   | Pre-aggregated summary tables for dashboards and BI                         |

---

### Star Schema

| Table                    | Type     | Description                                           |
|--------------------------|----------|-------------------------------------------------------|
| `spond_analytics.v_d_teams`     | Dimension | Team metadata (country, activity, created date)       |
| `spond_analytics.v_d_events`    | Dimension | Event metadata (time, location, duration)             |
| `spond_analytics.v_d_membership` | Dimension | Member roles, joined_at, team                         |
| `spond_analytics.v_d_geo_looker`      | Dimension | Regional information (country/state bounding boxes derived)   |
| `spond_analytics.v_f_events_rsvp` | Fact | One row per event-rsvp-id             |
| `spond_analytics.v_f_event_enriched`       | Fact | event metrics realted columns              |

---

# Data Source Assumption:
Data can reside either in a PostgreSQL database (e.g., hosted on AWS) or in CSV format on S3.

# Ingestion Frequency:
The pipeline is designed to run daily, processing data for yesterday’s partition using batch processing.

# First Load vs Incremental Load:

During the first load, a full historical ingestion is performed to backfill the dataset.

From the next scheduled run onward, only the incremental data for the previous day is ingested.

Following Databases already created in metastore.

- spond_raw
- spond_clean
- spond-analytics
- spond-aggregates


## Features

### 1. Data Ingestion (`1_raw/`)

- Ingests data as-is from source (PostgreSQL or S3).
- Defines schemas for as provided in requirement.
  - `teams`
  - `memberships`
  - `events`
  - `event_rsvps`
- Schema validation and partitioning based on date column.
- Supports full and incremental loads using partition filters.
- Data is stored in Delta format with p_date partitioning.

### 2. Data Cleaning & Validation (`2_clean/`)

- Ensures primary key is not null and unique.
- Validete foreign key consistency using joins.
- Validate the business logic for example `rsvp_status` or `country_code`.
- Adds validation flags (e.g., invalid timestamps, invalid FKs).
- Enriches tables with:
  - `event_duration_minutes`
  - `has_location` flags
  - `is_latest_response`
  - `is_valid` flags
  - Detailed `validation_error` messages

### 3. Dimensional Modeling (`3_dim/`)

- Builds star schema dimensions from clean stage.
  - `d_teams`
  - `d_membership`
  - `d_events`
- Adds audit columns: `etl_created_date`, `etl_updated_date`
- Uses overwrite for first load
- Uses SQL `MERGE` for incremental loads 
- Create Views 

### 4. Fact Tables (`4_fact/`)

- Creates `f_events_rsvps` fact table from cleaned RSVP data.
- Adds partitioning, metadata, and analytics-ready views.

- Creates `f_events_enriched` from dim and fact tables.
- enrich the table using d_team, d_geo_looker, f_events_rsvps and add calcluted metrics for fast analysis and reporting.

###  Analytics-Ready Design

Supports analytical queries like:

- **Daily Active Teams**:  d_event or `v_f_events_enriched`
- **RSVP Summary**: Status breakdown per event using `v_f_events_enriched`
- **Attendance Rate**: % of accepted RSVPs over total invites using `v_f_events_rsvps`
- **New vs Returning Members**: Based on weekly joins between `v_d_membership` and `v_f_events_rsvps`
can also be solved by planned `f_membership_enriched`
`week(joined_at)= week(responded_at)-->New member`
`week(joined_at) < week(responded_at)-->returning member`
- **Events per Region**: `f_events_enriched` has region column derived by joining `d_geo_looker` table.


---

## Data Views Documentation

This section provides definitions and schemas for the analytical views built on top of the Delta star schema. These views are optimized for BI tools and downstream consumption.

---

### View: `spond_analytics.v_d_teams`

**Description**  
This view provides metadata for teams, including team activity, country, and ETL tracking fields. It is derived from the `d_teams` Delta table.

**Schema**

| Column Name        | Data Type | Description                                                   |
|--------------------|-----------|---------------------------------------------------------------|
| `team_id`          | string    | Unique identifier for the team                                |
| `team_activity`    | string    | Activity type of the team                                     |
| `country_code`     | string    | Country code where the team is based                          |
| `created_at`       | timestamp | Timestamp when the team was created                           |
| `etl_created_date` | timestamp | Timestamp when the record was created in the ETL process      |
| `etl_updated_date` | timestamp | Timestamp when the record was last updated in the ETL process |

---

### View: `spond_analytics.v_d_membership`

**Description**  
This view provides membership details for teams, including role and join timestamp. It is sourced from the `d_membership` Delta table.

**Schema**

| Column Name        | Data Type | Description                                                   |
|--------------------|-----------|---------------------------------------------------------------|
| `membership_id`    | string    | Unique identifier for the membership                          |
| `team_id`          | string    | Identifier for the team this membership belongs to            |
| `role_title`       | string    | Title of the member's role (e.g., member, admin)              |
| `joined_at`        | timestamp | Timestamp when the membership started                         |
| `etl_created_date` | timestamp | Timestamp when the record was created in the ETL process      |
| `etl_updated_date` | timestamp | Timestamp when the record was last updated in the ETL process |

---

### View: `spond_analytics.v_d_events`

**Description**  
This view provides metadata about events, including time, duration, and location details. It supports time-based and geospatial analysis.

**Schema**

| Column Name             | Data Type | Description                                                   |
|-------------------------|-----------|---------------------------------------------------------------|
| `event_id`              | string    | Unique identifier for the event                               |
| `team_id`               | string    | Identifier of the team hosting the event                      |
| `event_start`           | timestamp | Start time of the event                                       |
| `event_end`             | timestamp | End time of the event                                         |
| `event_duration_minutes`| double    | Duration of the event in minutes                              |
| `has_location`          | boolean   | Flag indicating if the event has a geolocation                |
| `latitude`              | double    | Latitude of the event’s location                              |
| `longitude`             | double    | Longitude of the event’s location                             |
| `created_at`            | timestamp | Timestamp when the event was created                          |
| `etl_created_date`      | timestamp | Timestamp when the record was created in the ETL process      |
| `etl_updated_date`      | timestamp | Timestamp when the record was last updated in the ETL process |

---

### View: `spond_analytics.v_f_events_rsvps`

**Description**  
This view provides RSVP details for events, capturing each member's response status, timing, and ETL audit fields.

**Schema**

| Column Name           | Data Type | Description                                                   |
|------------------------|-----------|---------------------------------------------------------------|
| `event_id`            | string    | Unique identifier for the event                               |
| `team_id`             | string    | Unique identifier for the team                                |
| `rsvp_status`         | int       | RSVP status (`0 = unanswered`, `1 = accepted`, `2 = declined`)|
| `responded_at`        | timestamp | Timestamp when the RSVP was responded                         |
| `is_latest_response`  | boolean   | Flag indicating if this is the most recent RSVP               |
| `p_date`              | date      | Partition date                                                |
| `etl_created_date`    | timestamp | Timestamp when the record was created in the ETL process      |
| `etl_updated_date`    | timestamp | Timestamp when the record was last updated in the ETL process |

---

### View: `spond_analytics.v_f_events_enriched`

**Description**  
This view provides enriched event data combining event metadata with RSVP summary metrics and regional information.

**Schema**

| Column Name             | Data Type      | Description                                                   |
|--------------------------|----------------|---------------------------------------------------------------|
| `event_id`              | string         | Unique identifier for the event                               |
| `team_id`               | string         | Unique identifier for the team                                |
| `event_start`           | timestamp      | Start time of the event                                       |
| `event_end`             | timestamp      | End time of the event                                         |
| `event_date`            | date           | Date of the event (p_date)                                            |
| `event_duration_minutes`| double         | Duration of the event in minutes                              |
| `team_activity`         | string         | Activity of the team                                          |
| `state`                 | string         | State or region where the event took place                    |
| `country_code`          | string         | Country code of the event location                            |
| `total_rsvps`           | bigint         | Total number of RSVP invitations                             |
| `accepted_rsvps`        | bigint         | Number of accepted RSVPs                                     |
| `declined_rsvps`        | bigint         | Number of declined RSVPs                                     |
| `unanswered_rsvps`      | bigint         | Number of unanswered RSVPs                                   |
| `responded_members`     | bigint         | Number of members who responded                              |
| `event_attendance_rate` | decimal(27,2)  | Percentage of accepted RSVPs vs total RSVPs                  |
| `etl_created_date`    | timestamp | Timestamp when the record was created in the ETL process      |

---

## Data Quality Checks

### 1. Foreign Key Violations
- **Membership**: Found invalid `team_id`.
- **Events RSVP**: Found invalid `event_id` and `membership_id`.

### 2. Timestamp Inconsistencies
- **Events**: One row where `event_start > event_end`.

### 3. Duplicate RSVP Status
- **Event RSVP**: Members have two `rsvp_status` for the same event.  
  - Introduced `is_latest_flag` to handle this issue.

### 4. Nulls in Primary Identifiers
- No issues found.

### 5. Deduplication of Event-Level Rows
- No issues found.

### 6. Malformed Data Check
- Validated data against:
  - Allowed values for `rsvp_status` (0, 1, 2).
  - Country codes.

---

### Future Improvements & Enhancements

- Introduce **job-level parameters** to make the pipeline more configurable and reusable.
- Move current **validation SQL logic** out of notebooks and replace with proper **unit tests** for better automation and CI compatibility.
- Implement a dedicated **pipeline to reprocess invalid records**, using the `is_valid` flag and partition-aware revalidation logic.
- Add a **Time Dimension (dim_date)** to support advanced time-based analytics (daily, weekly, monthly, yearly trends).
- Build an **enriched fact table for memberships** to provide lifecycle and engagement metrics per member.
- Create an **aggregation layer** to pre-compute weekly/monthly metrics for faster reporting and exploration.
- Set up a **CI pipeline** to run tests and validations automatically on each deployment.
- Optimize storage and performance by:
  - Applying appropriate **partitioning** strategies  
  - Using **Z-Ordering** on `event_id`, `team_id`, and `membership_id`  
  - Reviewing **clustering strategies**
- Define and apply **data retention policies** to minimize long-term storage costs.
- Review and revise **cluster policies and repartitioning strategies** to reduce compute usage and improve cost efficiency.
- Continue evolving the **data model based on business analytical use cases**, ensuring it stays flexible and scalable.
- Implement **user access control** on views and tables to support secure data sharing and role-based access.

---

## Scheduling & Deployment (Planned)

- DAG runs daily in Airflow
- Task execution via Databricks Jobs
- Logging, retries, and alerting via task config

## Summary

This project was implemented entirely using **Delta Lake and PySpark**, following **dbt-style modular modeling principles** (`raw → clean → dim/fact → aggregates`). It is fully compatible with dbt and can be easily migrated in the future if needed.