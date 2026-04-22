/* @bruin

name: marts.mart_pm25_geo_summary
type: duckdb.sql
connection: motherduck-default
description: |
  Dashboard-ready PM2.5 geography summary table for state and county views.
  Each row represents one geography-year combination with average site-level
  PM2.5 concentration, worst site concentration, monitoring site count, and
  a within-level ranking for the selected year.

  The asset is designed to back dashboard tables, leaderboards, and filterable
  summary cards without forcing the frontend to re-aggregate site-level data.
  It uses the same site-level PM2.5 grain as the core model, then rolls that up
  to state and county levels in a single unified table.

  Non-US and ambiguous geographies are excluded to keep dashboard outputs focused
  on US analysis. County rows require a non-null county name.
tags:
  - domain:environmental
  - data_type:mart
  - source:epa_aqs
  - granularity:geography_year
  - pipeline_role:mart
  - refresh_pattern:monthly
  - sensitivity:public
  - pollutant:pm25
  - integration:dashboard_ready
  - use_case:leaderboard
  - scope:state_and_county

materialization:
  type: table

depends:
  - core.fct_measurements
  - core.dim_site

columns:
  - name: pollutant
    type: varchar
    description: Fixed pollutant label for this mart
    checks:
      - name: not_null
      - name: accepted_values
        value: ["PM2.5"]

  - name: geography_level
    type: varchar
    description: Summary grain used by the dashboard
    checks:
      - name: not_null
      - name: accepted_values
        value: ["state", "county"]

  - name: geography_name
    type: varchar
    description: Display-friendly geography label for the selected level
    checks:
      - name: not_null

  - name: state_name
    type: varchar
    description: State name for the row
    checks:
      - name: not_null

  - name: county_name
    type: varchar
    description: County name when geography_level = county, else null

  - name: year
    type: integer
    description: Calendar year of the PM2.5 summary
    checks:
      - name: not_null
      - name: accepted_values
        value: [2019, 2020, 2021, 2022, 2023]

  - name: is_latest_year
    type: boolean
    description: True for the most recent year available in the mart
    checks:
      - name: not_null

  - name: avg_mean_value
    type: double
    description: Average site-level PM2.5 concentration for the geography-year
    checks:
      - name: not_null
      - name: non_negative

  - name: max_site_value
    type: double
    description: Highest site-level PM2.5 concentration inside the geography-year
    checks:
      - name: not_null
      - name: non_negative

  - name: site_count
    type: integer
    description: Number of distinct monitoring sites in the geography-year
    checks:
      - name: not_null
      - name: positive

  - name: geography_rank
    type: bigint
    description: Rank within geography_level and year by avg_mean_value descending
    checks:
      - name: not_null
      - name: positive

  - name: unit
    type: varchar
    description: EPA unit of measure for PM2.5 concentrations
    checks:
      - name: not_null

@bruin */

WITH site_pm25 AS (
    SELECT
        f.year,
        f.site_id,
        d.state_name,
        d.county_name,
        f.mean_value,
        f.unit
    FROM core.fct_measurements f
    JOIN core.dim_site d USING (site_id)
    WHERE f.pollutant = 'PM2.5'
      AND d.state_name NOT IN ('Country Of Mexico', 'Virgin Islands', 'Unknown')
),

state_summary AS (
    SELECT
        'PM2.5'                  AS pollutant,
        'state'                  AS geography_level,
        state_name               AS geography_name,
        state_name,
        CAST(NULL AS VARCHAR)    AS county_name,
        year,
        ROUND(AVG(mean_value), 4) AS avg_mean_value,
        ROUND(MAX(mean_value), 4) AS max_site_value,
        COUNT(DISTINCT site_id)   AS site_count,
        MAX(unit)                 AS unit
    FROM site_pm25
    GROUP BY state_name, year
),

county_summary AS (
    SELECT
        'PM2.5'                  AS pollutant,
        'county'                 AS geography_level,
        county_name || ', ' || state_name AS geography_name,
        state_name,
        county_name,
        year,
        ROUND(AVG(mean_value), 4) AS avg_mean_value,
        ROUND(MAX(mean_value), 4) AS max_site_value,
        COUNT(DISTINCT site_id)   AS site_count,
        MAX(unit)                 AS unit
    FROM site_pm25
    WHERE county_name IS NOT NULL
      AND TRIM(county_name) <> ''
    GROUP BY state_name, county_name, year
),

combined AS (
    SELECT * FROM state_summary
    UNION ALL
    SELECT * FROM county_summary
)

SELECT
    pollutant,
    geography_level,
    geography_name,
    state_name,
    county_name,
    year,
    year = MAX(year) OVER () AS is_latest_year,
    avg_mean_value,
    max_site_value,
    site_count,
    RANK() OVER (
        PARTITION BY geography_level, year
        ORDER BY avg_mean_value DESC, geography_name ASC
    ) AS geography_rank,
    unit
FROM combined
ORDER BY year DESC, geography_level, geography_rank
