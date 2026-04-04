/* @bruin

name: core.fct_measurements
type: motherduck.sql
description: |
  Central fact table containing aggregated EPA Air Quality System (AQS) measurements for the five major criteria pollutants monitored across the United States from 2019-2023.

  This table represents the core analytical layer that aggregates raw monitoring data to one record per site-pollutant-year combination. When multiple monitoring instruments (POCs) exist at a single site for the same pollutant, the mean_value is averaged across all instruments to provide a unified annual concentration for analytical purposes.

  The data undergoes quality filtering to exclude null, negative, or invalid measurements, ensuring all values represent legitimate air quality readings. The table serves as the primary source for national air quality trend analysis, state comparisons, and pollutant ranking reports in downstream mart tables.

  Key business insights: Provides foundation for understanding long-term air quality trends, regulatory compliance monitoring, and public health impact assessments across different geographic regions and pollutant types.
tags:
  - domain:environmental
  - data_type:fact_table
  - source:epa_aqs
  - granularity:annual
  - pipeline_role:core
  - refresh_pattern:monthly
  - sensitivity:public
  - quality:high_governance

materialization:
  type: table

depends:
  - staging.stg_measurements
  - core.dim_site

columns:
  - name: measurement_id
    type: VARCHAR
    description: |
      Surrogate primary key constructed as site_id || '__' || pollutant || '__' || year.
      Unique identifier ensuring one record per monitoring site-pollutant-year combination.
      Example format: '01_073_0023__PM2.5__2023'
    checks:
      - name: not_null
      - name: unique
  - name: site_id
    type: VARCHAR
    description: |
      Foreign key reference to core.dim_site. Composite identifier in format 'state_county_site'
      using EPA standardized codes (e.g., '06_037_1103' for Los Angeles). Links to geographic
      and location metadata for spatial analysis.
    checks:
      - name: not_null
  - name: pollutant
    type: VARCHAR
    description: |
      Standardized pollutant identifier for the five major criteria air pollutants regulated
      under the Clean Air Act. Normalized from verbose EPA parameter names to short codes
      for consistent analysis across different monitoring programs.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - PM2.5
          - Ozone
          - CO
          - NO2
          - SO2
  - name: year
    type: INTEGER
    description: |
      Calendar year of the measurement (2019-2023 range). Represents the annual reporting
      period for EPA AQS data. Used for temporal trend analysis and year-over-year comparisons.
    checks:
      - name: not_null
  - name: mean_value
    type: DOUBLE
    description: |
      Annual arithmetic mean concentration averaged across all monitoring instruments (POCs)
      at the site for the given pollutant-year combination. Represents the primary air quality
      metric used for regulatory compliance and health impact assessments.
      Units vary by pollutant: µg/m³ for particulates, ppm for gases.
    checks:
      - name: not_null
      - name: non_negative
  - name: unit
    type: VARCHAR
    description: |
      Unit of measurement for the concentration value. Standardized EPA units:
      'Micrograms/cubic meter (LC)' for PM2.5, 'Parts per million' for gaseous pollutants.
      Critical for proper interpretation and comparison of concentration levels across pollutants.
  - name: parameter_name
    type: VARCHAR
    description: |-
      Original EPA parameter name from source data (e.g., 'PM2.5 - Local Conditions',
      'Carbon monoxide'). Preserved for traceability back to EPA AQS official parameter
      definitions and regulatory reference documentation.

@bruin */

-- Aggregate to one row per site+pollutant+year (average across POC/observation periods)
SELECT
    site_id || '__' || pollutant || '__' || CAST(year AS VARCHAR) AS measurement_id,
    site_id,
    pollutant,
    MAX(parameter_name)          AS parameter_name,
    year,
    AVG(mean_value)              AS mean_value,
    MAX(unit)                    AS unit

FROM staging.stg_measurements

WHERE
    site_id   IS NOT NULL
    AND pollutant  IS NOT NULL
    AND year       IS NOT NULL
    AND mean_value IS NOT NULL
    AND mean_value >= 0

GROUP BY site_id, pollutant, year
