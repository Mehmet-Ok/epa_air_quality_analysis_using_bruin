/* @bruin

name: marts.mart_aqi_annual_trends
type: duckdb.sql
connection: motherduck-default
description: |
  National-level annual air quality trends summary for the five major EPA criteria pollutants (PM2.5, Ozone, CO, NO2, SO2) from 2019-2023.

  This mart aggregates site-level measurements to provide a national perspective on air quality patterns, calculating key statistical measures including central tendencies and distribution percentiles. The table enables trend analysis through year-over-year percentage change calculations, making it the primary resource for understanding long-term national air quality improvements or deterioration.

  Each row represents one pollutant-year combination with national statistics computed across all EPA AQS monitoring sites. The year-over-year change metric uses the previous year as baseline, so 2019 data will have NULL yoy_change_pct values. Statistical measures are rounded to 4 decimal places for concentration values and 2 decimal places for percentage changes to balance precision with readability.

  Primary use cases include regulatory compliance reporting, public health trend assessment, environmental policy impact evaluation, and comparative analysis across pollutant types. The table serves data consumers ranging from EPA analysts to environmental researchers and public health officials.
tags:
  - domain:environmental
  - data_type:mart
  - source:epa_aqs
  - granularity:annual
  - pipeline_role:mart
  - refresh_pattern:monthly
  - sensitivity:public
  - analysis_type:trend_analysis
  - scope:national
  - update_pattern:append_only

materialization:
  type: table

depends:
  - core.fct_measurements

columns:
  - name: pollutant
    type: VARCHAR
    description: |
      Standardized pollutant identifier (PM2.5, Ozone, CO, NO2, SO2). These represent the five major criteria air pollutants
      regulated under the Clean Air Act. Normalized from verbose EPA parameter names to enable consistent analysis and reporting
      across different monitoring programs and time periods.
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
      Calendar year of the aggregated measurements (2019-2023 coverage). Represents the EPA AQS annual reporting period.
      Used as the temporal dimension for trend analysis and year-over-year comparison calculations.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - 2019
          - 2020
          - 2021
          - 2022
          - 2023
  - name: avg_mean_value
    type: DOUBLE
    description: |
      National arithmetic mean of site-level annual concentration averages, rounded to 4 decimal places. Computed by averaging
      the mean_value from all EPA monitoring sites for the given pollutant-year combination. Units vary by pollutant:
      µg/m³ for PM2.5, ppm for gaseous pollutants (Ozone, CO, NO2, SO2). This is the primary metric for national air quality
      trend assessment and regulatory compliance evaluation.
    checks:
      - name: not_null
      - name: non_negative
  - name: median_mean_value
    type: DOUBLE
    description: |
      National median of site-level annual concentration averages, rounded to 4 decimal places. Provides a measure of central
      tendency less sensitive to outlier monitoring sites than the arithmetic mean. Useful for understanding typical air quality
      conditions while minimizing the influence of extremely high or low measurement sites that may skew the national average.
    checks:
      - name: not_null
      - name: non_negative
  - name: p10_value
    type: DOUBLE
    description: |
      10th percentile of site-level annual concentration averages, rounded to 4 decimal places. Represents the concentration
      level below which 10% of monitoring sites fall, effectively indicating the cleanest air quality conditions across the
      national monitoring network. Useful for identifying best-case air quality scenarios and setting improvement targets.
    checks:
      - name: not_null
      - name: non_negative
  - name: p90_value
    type: DOUBLE
    description: |
      90th percentile of site-level annual concentration averages, rounded to 4 decimal places. Represents the concentration
      level above which only 10% of monitoring sites exceed, effectively indicating areas with the highest pollution burden.
      Critical for identifying environmental justice concerns and prioritizing regulatory intervention in the most affected regions.
    checks:
      - name: not_null
      - name: non_negative
  - name: site_count
    type: BIGINT
    description: |
      Total number of distinct EPA AQS monitoring sites contributing data for the pollutant-year combination. Indicates the
      statistical robustness of the national aggregates - higher site counts generally provide more reliable trend indicators.
      Changes in site_count over time may reflect expansion or reduction of the monitoring network, which should be considered
      when interpreting year-over-year trends.
    checks:
      - name: not_null
      - name: positive
  - name: yoy_change_pct
    type: DOUBLE
    description: |-
      Year-over-year percentage change in the national average concentration (avg_mean_value), rounded to 2 decimal places.
      Calculated as ((current_year - previous_year) / previous_year) * 100. NULL for the first year of each pollutant (2019)
      since no prior baseline exists. Negative values indicate air quality improvement (decreasing pollution), while positive
      values suggest deterioration. This is the primary metric for assessing the effectiveness of air quality regulations and policies.

@bruin */

WITH base AS (
    SELECT
        pollutant,
        year,
        AVG(mean_value)                          AS avg_mean_value,
        MEDIAN(mean_value)                       AS median_mean_value,
        PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY mean_value) AS p10_value,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mean_value) AS p90_value,
        COUNT(DISTINCT site_id)                  AS site_count
    FROM core.fct_measurements
    GROUP BY pollutant, year
)

SELECT
    pollutant,
    year,
    ROUND(avg_mean_value,    4) AS avg_mean_value,
    ROUND(median_mean_value, 4) AS median_mean_value,
    ROUND(p10_value,         4) AS p10_value,
    ROUND(p90_value,         4) AS p90_value,
    site_count,
    ROUND(
        100.0 * (avg_mean_value - LAG(avg_mean_value) OVER (PARTITION BY pollutant ORDER BY year))
            / NULLIF(LAG(avg_mean_value) OVER (PARTITION BY pollutant ORDER BY year), 0),
        2
    )                                            AS yoy_change_pct

FROM base
ORDER BY pollutant, year
