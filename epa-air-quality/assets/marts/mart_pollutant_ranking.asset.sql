/* @bruin

name: marts.mart_pollutant_ranking
type: motherduck.sql
description: |
  Comprehensive air quality ranking analysis that ranks EPA monitoring sites by pollutant concentration levels both nationally and within each state for the five major criteria pollutants (PM2.5, Ozone, CO, NO2, SO2) from 2019-2023.

  This mart provides critical insights into which geographic areas experience the highest pollution burden by creating dual ranking systems. National rankings identify the worst air quality sites across the entire United States, while state rankings enable within-state comparisons and identification of local pollution hotspots. Higher ranks indicate higher pollutant concentrations and worse air quality conditions.

  The ranking methodology uses dense ranking based on annual mean concentrations, meaning sites with identical concentration values receive the same rank. This approach ensures fair comparison while handling cases where multiple monitoring sites may have exactly the same measured values due to rounding or measurement precision limitations.

  Key business applications include environmental justice analysis (identifying disproportionately affected communities), regulatory enforcement prioritization, public health advisory targeting, and comparative performance assessment between states. The dual ranking structure enables both broad national policy insights and granular local environmental management decisions.

  Data refresh occurs monthly as part of the EPA AQS pipeline, with rankings recalculated across the complete historical dataset to ensure consistency when new annual data becomes available. Each pollutant maintains separate ranking distributions due to different concentration scales and health impact thresholds.
tags:
  - domain:environmental
  - data_type:mart
  - source:epa_aqs
  - granularity:annual
  - pipeline_role:mart
  - refresh_pattern:monthly
  - sensitivity:public
  - analysis_type:ranking_analysis
  - scope:national_and_state
  - update_pattern:snapshot
  - use_case:pollution_hotspots

materialization:
  type: table

depends:
  - core.fct_measurements
  - core.dim_site

columns:
  - name: site_id
    type: VARCHAR
    description: |
      Foreign key reference to core.dim_site. EPA monitoring site identifier in composite format 'state_county_site'
      using standardized EPA codes (e.g., '06_037_1103' for Los Angeles). Links to detailed geographic metadata for
      spatial analysis and location-based filtering. Essential for mapping pollution hotspots and understanding
      geographic distribution of air quality impacts.
    checks:
      - name: not_null
  - name: state_name
    type: VARCHAR
    description: |
      Full US state name derived from EPA geographic references. Used for state-level aggregation and filtering in
      analysis workflows. Enables state-to-state air quality comparisons and supports state-specific regulatory
      reporting requirements. Standardized to official state names for consistency across the dataset.
    checks:
      - name: not_null
  - name: county_name
    type: VARCHAR
    description: |
      County name within the state where the monitoring site is located. Provides sub-state geographic granularity
      for local air quality analysis. Critical for identifying county-level pollution patterns and supporting
      local government environmental planning efforts. Some sites may have NULL county designations for special
      monitoring locations.
  - name: pollutant
    type: VARCHAR
    description: |
      Standardized EPA criteria pollutant identifier (PM2.5, Ozone, CO, NO2, SO2). These five pollutants represent
      the major air quality indicators regulated under the Clean Air Act due to their significant public health impacts.
      Rankings are calculated independently for each pollutant due to different concentration scales, health thresholds,
      and regulatory standards.
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
      Calendar year of the air quality measurement (2019-2023 coverage). Rankings are calculated independently within
      each year to account for temporal variations in air quality conditions, regulatory changes, and monitoring network
      adjustments. Used for trend analysis to identify whether high-ranking sites consistently show poor air quality
      over time.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - 2019
          - 2020
          - 2021
          - 2022
          - 2023
  - name: mean_value
    type: DOUBLE
    description: |
      Annual arithmetic mean pollutant concentration in standardized units, rounded to 4 decimal places. This is the
      primary metric used for ranking calculations. Values represent site-level averages across all monitoring
      instruments and observation periods during the year. Units vary by pollutant: µg/m³ for PM2.5, ppm for gaseous
      pollutants (Ozone, CO, NO2, SO2). Higher values indicate worse air quality conditions.
    checks:
      - name: not_null
      - name: non_negative
  - name: unit
    type: VARCHAR
    description: |
      Standardized EPA unit of measurement for the concentration value. Critical for proper interpretation of
      rankings and concentration comparisons. 'Micrograms/cubic meter (LC)' for particulate matter (PM2.5),
      'Parts per million' for gaseous pollutants. Units remain consistent within each pollutant type but vary
      across different pollutant categories due to measurement methodology differences.
    checks:
      - name: not_null
  - name: national_rank
    type: BIGINT
    description: |
      Dense rank of the monitoring site within all US sites for the specific pollutant-year combination, ordered by
      descending concentration (rank 1 = highest concentration = worst air quality). Enables identification of the
      most polluted locations nationally and supports federal environmental justice initiatives. Tied concentrations
      receive identical ranks, with subsequent ranks continuing sequentially without gaps.
    checks:
      - name: not_null
      - name: positive
  - name: state_rank
    type: BIGINT
    description: |
      Dense rank of the monitoring site within its state for the specific pollutant-year combination, ordered by
      descending concentration (rank 1 = highest concentration within state = worst state-level air quality).
      Enables state-level environmental management and supports local air quality improvement targeting.
      Essential for intrastate comparisons and state regulatory prioritization efforts.
    checks:
      - name: not_null
      - name: positive

@bruin */

SELECT
    f.site_id,
    d.state_name,
    d.county_name,
    f.pollutant,
    f.year,
    ROUND(f.mean_value, 4)                                                         AS mean_value,
    f.unit,
    RANK() OVER (PARTITION BY f.pollutant, f.year ORDER BY f.mean_value DESC)      AS national_rank,
    RANK() OVER (PARTITION BY d.state_name, f.pollutant, f.year
                 ORDER BY f.mean_value DESC)                                        AS state_rank

FROM core.fct_measurements f
JOIN core.dim_site d USING (site_id)

ORDER BY f.pollutant, f.year, national_rank
