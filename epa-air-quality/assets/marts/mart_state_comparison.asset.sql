/* @bruin

name: marts.mart_state_comparison
type: motherduck.sql
description: |
  State-level air quality analysis table comparing each US state's pollution performance against national averages for EPA criteria pollutants (2019-2023).

  This mart aggregates monitoring site data to the state level, providing key metrics for environmental policy analysis, public health assessments, and regulatory compliance tracking. Each row represents one state-pollutant-year combination with comparative statistics.

  The table enables identification of the most and least polluted states, tracks state-level air quality trends over time, and quantifies how far each state deviates from national pollution averages. State rankings are calculated within each pollutant-year combination to facilitate cross-state comparisons.

  Key use cases: State environmental scorecards, regulatory compliance monitoring, public health impact assessments, policy effectiveness evaluation, and identifying states requiring targeted air quality interventions.

  Data quality notes: States with fewer than one monitoring site for a given pollutant-year may show less reliable averages. Alaska and Hawaii may have limited monitoring coverage compared to continental US states.
tags:
  - domain:environmental
  - data_type:mart_table
  - source:epa_aqs
  - granularity:state_annual
  - pipeline_role:mart
  - refresh_pattern:monthly
  - sensitivity:public
  - analytics_use:comparative_analysis
  - governance:regulatory_compliance
  - geographic_scope:us_states

materialization:
  type: table

depends:
  - core.fct_measurements
  - core.dim_site

columns:
  - name: state_name
    type: VARCHAR
    description: |
      Official US state name (e.g., 'California', 'Texas'). All 50 states plus DC and territories
      with EPA monitoring sites included. Used for geographic filtering and reporting breakdowns.
    checks:
      - name: not_null
  - name: pollutant
    type: VARCHAR
    description: |
      EPA criteria pollutant identifier standardized to short codes. One of five major air quality
      indicators regulated under the Clean Air Act: PM2.5 (fine particulate matter), Ozone (ground-level),
      CO (carbon monoxide), NO2 (nitrogen dioxide), SO2 (sulfur dioxide). Critical for pollutant-specific analysis.
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
      Calendar year of measurements (2019-2023 range). Represents the annual EPA AQS reporting period.
      Used for temporal trend analysis and year-over-year state performance comparisons.
    checks:
      - name: not_null
  - name: avg_mean_value
    type: DOUBLE
    description: |
      State-level arithmetic mean of all monitoring sites' annual average concentrations within the state.
      Represents the typical air quality across the state for the given pollutant-year. Units vary by
      pollutant (µg/m³ for PM2.5, ppm for gases). This is the primary metric for state air quality assessment.
    checks:
      - name: not_null
      - name: non_negative
  - name: max_mean_value
    type: DOUBLE
    description: |
      Highest annual average concentration recorded at any monitoring site within the state for the
      given pollutant-year. Identifies pollution hotspots and worst-case air quality conditions within
      each state. Critical for understanding intra-state pollution variability.
    checks:
      - name: not_null
      - name: non_negative
  - name: site_count
    type: BIGINT
    description: |
      Number of EPA monitoring sites reporting valid annual data within the state for the specific
      pollutant-year combination. Indicates data reliability and geographic coverage density.
      Higher counts suggest more representative state averages.
    checks:
      - name: not_null
      - name: positive
  - name: national_avg
    type: DOUBLE
    description: |
      National arithmetic mean concentration for the same pollutant-year combination across all US
      monitoring sites. Baseline metric for comparative analysis enabling identification of states
      performing better or worse than the national average.
    checks:
      - name: not_null
      - name: non_negative
  - name: pct_above_national
    type: DOUBLE
    description: |
      Percentage deviation of state average from national average ((state_avg - national_avg) / national_avg * 100).
      Positive values indicate above-national pollution levels, negative values indicate below-national levels.
      Key metric for identifying environmental leaders and laggards among states.
  - name: state_rank
    type: BIGINT
    description: |
      State ranking within each pollutant-year combination ordered by avg_mean_value (1 = highest pollution,
      50+ = lowest pollution). Enables easy identification of most and least polluted states for each
      pollutant-year. Essential for state-to-state competitive analysis and policy benchmarking.
    checks:
      - name: not_null
      - name: positive

@bruin */

WITH state_agg AS (
    SELECT
        d.state_name,
        f.pollutant,
        f.year,
        ROUND(AVG(f.mean_value), 4)  AS avg_mean_value,
        ROUND(MAX(f.mean_value), 4)  AS max_mean_value,
        COUNT(DISTINCT f.site_id)    AS site_count
    FROM core.fct_measurements f
    JOIN core.dim_site d USING (site_id)
    GROUP BY d.state_name, f.pollutant, f.year
),

national_avg AS (
    SELECT
        pollutant,
        year,
        ROUND(AVG(mean_value), 4) AS national_avg
    FROM core.fct_measurements
    GROUP BY pollutant, year
)

SELECT
    s.state_name,
    s.pollutant,
    s.year,
    s.avg_mean_value,
    s.max_mean_value,
    s.site_count,
    n.national_avg,
    ROUND(100.0 * (s.avg_mean_value - n.national_avg) / NULLIF(n.national_avg, 0), 2) AS pct_above_national,
    RANK() OVER (PARTITION BY s.pollutant, s.year ORDER BY s.avg_mean_value DESC)       AS state_rank

FROM state_agg    s
JOIN national_avg n USING (pollutant, year)

ORDER BY s.pollutant, s.year, state_rank
