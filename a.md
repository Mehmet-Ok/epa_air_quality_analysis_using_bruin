mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/core/fct_measurements.asset.sql

✓ Enhanced 'fct_measurements.asset.sql'

Changes:
  /* @bruin
mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/core/fct_measurements.asset.sql

✓ Enhanced 'fct_measurements.asset.sql'

Changes:
  /* @bruin

  name: core.fct_measurements
  type: duckdb.sql
+ description: |
+   Central fact table containing aggregated EPA Air Quality System (AQS) measurements for the five major criteria pollutants monitored across the United States from 2019-2023.
+
+   This table represents the core analytical layer that aggregates raw monitoring data to one record per site-pollutant-year combination. When multiple monitoring instruments (POCs) exist at a single site for the same pollutant, the mean_value is averaged across all instruments to provide a unified annual concentration for analytical purposes.
+
+   The data undergoes quality filtering to exclude null, negative, or invalid measurements, ensuring all values represent legitimate air quality readings. The table serves as the primary source for national air quality trend analysis, state comparisons, and pollutant ranking reports in downstream mart tables.
+
+   Key business insights: Provides foundation for understanding long-term air quality trends, regulatory compliance monitoring, and public health impact assessments across different geographic regions and pollutant types.
+ tags:
+   - domain:environmental
+   - data_type:fact_table
+   - source:epa_aqs
+   - granularity:annual
+   - pipeline_role:core
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - quality:high_governance

  materialization:
    type: table

  depends:
    - staging.stg_measurements
    - core.dim_site

  columns:
    - name: measurement_id
      type: VARCHAR
-     description: Surrogate key (site_id + pollutant + year)
+     description: |
+       Surrogate primary key constructed as site_id || '__' || pollutant || '__' || year.
+       Unique identifier ensuring one record per monitoring site-pollutant-year combination.
+       Example format: '01_073_0023__PM2.5__2023'
      checks:
        - name: not_null
        - name: unique
    - name: site_id
      type: VARCHAR
-     description: Foreign key to core.dim_site
+     description: |
+       Foreign key reference to core.dim_site. Composite identifier in format 'state_county_site'
+       using EPA standardized codes (e.g., '06_037_1103' for Los Angeles). Links to geographic
+       and location metadata for spatial analysis.
      checks:
        - name: not_null
    - name: pollutant
      type: VARCHAR
-     description: Normalized pollutant short name
+     description: |
+       Standardized pollutant identifier for the five major criteria air pollutants regulated
+       under the Clean Air Act. Normalized from verbose EPA parameter names to short codes
+       for consistent analysis across different monitoring programs.
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
-     description: Measurement year
+     description: |
+       Calendar year of the measurement (2019-2023 range). Represents the annual reporting
+       period for EPA AQS data. Used for temporal trend analysis and year-over-year comparisons.
      checks:
        - name: not_null
    - name: mean_value
      type: DOUBLE
-     description: Annual arithmetic mean concentration
+     description: |
+       Annual arithmetic mean concentration averaged across all monitoring instruments (POCs)
+       at the site for the given pollutant-year combination. Represents the primary air quality
+       metric used for regulatory compliance and health impact assessments.
+       Units vary by pollutant: µg/m³ for particulates, ppm for gases.
      checks:
        - name: not_null
        - name: non_negative
    - name: unit
      type: VARCHAR
-     description: Unit of measure
+     description: |
+       Unit of measurement for the concentration value. Standardized EPA units:
+       'Micrograms/cubic meter (LC)' for PM2.5, 'Parts per million' for gaseous pollutants.
+       Critical for proper interpretation and comparison of concentration levels across pollutants.
    - name: parameter_name
      type: VARCHAR
+     description: |-
+       Original EPA parameter name from source data (e.g., 'PM2.5 - Local Conditions',
+       'Carbon monoxide'). Preserved for traceability back to EPA AQS official parameter
+       definitions and regulatory reference documentation.

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

+45 additions, -6 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/marts/mart_state_comparison.asset.sql

✓ Enhanced 'mart_state_comparison.asset.sql'

Changes:
  /* @bruin

  name: marts.mart_state_comparison
  type: duckdb.sql
+ description: |
+   State-level air quality analysis table comparing each US state's pollution performance against national averages for EPA criteria pollutants (2019-2023).
+
+   This mart aggregates monitoring site data to the state level, providing key metrics for environmental policy analysis, public health assessments, and regulatory compliance tracking. Each row represents one state-pollutant-year combination with comparative statistics.
+
+   The table enables identification of the most and least polluted states, tracks state-level air quality trends over time, and quantifies how far each state deviates from national pollution averages. State rankings are calculated within each pollutant-year combination to facilitate cross-state comparisons.
+
+   Key use cases: State environmental scorecards, regulatory compliance monitoring, public health impact assessments, policy effectiveness evaluation, and identifying states requiring targeted air quality interventions.
+
+   Data quality notes: States with fewer than one monitoring site for a given pollutant-year may show less reliable averages. Alaska and Hawaii may have limited monitoring coverage compared to continental US states.
+ tags:
+   - domain:environmental
+   - data_type:mart_table
+   - source:epa_aqs
+   - granularity:state_annual
+   - pipeline_role:mart
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - analytics_use:comparative_analysis
+   - governance:regulatory_compliance
+   - geographic_scope:us_states
+
  materialization:
    type: table

  depends:
    - core.fct_measurements
    - core.dim_site

  columns:
    - name: state_name
-     type: varchar
-     description: US state name
+     type: VARCHAR
+     description: |
+       Official US state name (e.g., 'California', 'Texas'). All 50 states plus DC and territories
+       with EPA monitoring sites included. Used for geographic filtering and reporting breakdowns.
      checks:
        - name: not_null
-
    - name: pollutant
-     type: varchar
-     description: Pollutant short name
+     type: VARCHAR
+     description: |
+       EPA criteria pollutant identifier standardized to short codes. One of five major air quality
+       indicators regulated under the Clean Air Act: PM2.5 (fine particulate matter), Ozone (ground-level),
+       CO (carbon monoxide), NO2 (nitrogen dioxide), SO2 (sulfur dioxide). Critical for pollutant-specific analysis.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of measurements (2019-2023 range). Represents the annual EPA AQS reporting period.
+       Used for temporal trend analysis and year-over-year state performance comparisons.
      checks:
        - name: not_null
-
    - name: avg_mean_value
-     type: double
-     description: State average of site-level annual means
-
+     type: DOUBLE
+     description: |
+       State-level arithmetic mean of all monitoring sites' annual average concentrations within the state.
+       Represents the typical air quality across the state for the given pollutant-year. Units vary by
+       pollutant (µg/m³ for PM2.5, ppm for gases). This is the primary metric for state air quality assessment.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: max_mean_value
-     type: double
-     description: Worst site reading in the state
-
+     type: DOUBLE
+     description: |
+       Highest annual average concentration recorded at any monitoring site within the state for the
+       given pollutant-year. Identifies pollution hotspots and worst-case air quality conditions within
+       each state. Critical for understanding intra-state pollution variability.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: site_count
-     type: integer
-     description: Number of monitoring sites in the state
-
+     type: BIGINT
+     description: |
+       Number of EPA monitoring sites reporting valid annual data within the state for the specific
+       pollutant-year combination. Indicates data reliability and geographic coverage density.
+       Higher counts suggest more representative state averages.
+     checks:
+       - name: not_null
+       - name: positive
    - name: national_avg
-     type: double
-     description: National average for the same pollutant and year
-
+     type: DOUBLE
+     description: |
+       National arithmetic mean concentration for the same pollutant-year combination across all US
+       monitoring sites. Baseline metric for comparative analysis enabling identification of states
+       performing better or worse than the national average.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: pct_above_national
-     type: double
-     description: How many % above/below the national average
-
+     type: DOUBLE
+     description: |
+       Percentage deviation of state average from national average ((state_avg - national_avg) / national_avg * 100).
+       Positive values indicate above-national pollution levels, negative values indicate below-national levels.
+       Key metric for identifying environmental leaders and laggards among states.
    - name: state_rank
-     type: integer
-     description: State rank (1 = most polluted) within pollutant+year
+     type: BIGINT
+     description: |
+       State ranking within each pollutant-year combination ordered by avg_mean_value (1 = highest pollution,
+       50+ = lowest pollution). Enables easy identification of most and least polluted states for each
+       pollutant-year. Essential for state-to-state competitive analysis and policy benchmarking.
+     checks:
+       - name: not_null
+       - name: positive

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

+87 additions, -26 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/marts/mart_aqi_annual_trends.asset.sql

✓ Enhanced 'mart_aqi_annual_trends.asset.sql'

Changes:
  /* @bruin

  name: marts.mart_aqi_annual_trends
  type: duckdb.sql
+ description: |
+   National-level annual air quality trends summary for the five major EPA criteria pollutants (PM2.5, Ozone, CO, NO2, SO2) from 2019-2023.
+
+   This mart aggregates site-level measurements to provide a national perspective on air quality patterns, calculating key statistical measures including central tendencies and distribution percentiles. The table enables trend analysis through year-over-year percentage change calculations, making it the primary resource for understanding long-term national air quality improvements or deterioration.
+
+   Each row represents one pollutant-year combination with national statistics computed across all EPA AQS monitoring sites. The year-over-year change metric uses the previous year as baseline, so 2019 data will have NULL yoy_change_pct values. Statistical measures are rounded to 4 decimal places for concentration values and 2 decimal places for percentage changes to balance precision with readability.
+
+   Primary use cases include regulatory compliance reporting, public health trend assessment, environmental policy impact evaluation, and comparative analysis across pollutant types. The table serves data consumers ranging from EPA analysts to environmental researchers and public health officials.
+ tags:
+   - domain:environmental
+   - data_type:mart
+   - source:epa_aqs
+   - granularity:annual
+   - pipeline_role:mart
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - analysis_type:trend_analysis
+   - scope:national
+   - update_pattern:append_only
+
  materialization:
    type: table

  depends:
    - core.fct_measurements

  columns:
    - name: pollutant
-     type: varchar
-     description: Pollutant short name
+     type: VARCHAR
+     description: |
+       Standardized pollutant identifier (PM2.5, Ozone, CO, NO2, SO2). These represent the five major criteria air pollutants
+       regulated under the Clean Air Act. Normalized from verbose EPA parameter names to enable consistent analysis and reporting
+       across different monitoring programs and time periods.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of the aggregated measurements (2019-2023 coverage). Represents the EPA AQS annual reporting period.
+       Used as the temporal dimension for trend analysis and year-over-year comparison calculations.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - 2019
+           - 2020
+           - 2021
+           - 2022
+           - 2023
    - name: avg_mean_value
-     type: double
-     description: National average of site-level annual means
-
+     type: DOUBLE
+     description: |
+       National arithmetic mean of site-level annual concentration averages, rounded to 4 decimal places. Computed by averaging
+       the mean_value from all EPA monitoring sites for the given pollutant-year combination. Units vary by pollutant:
+       µg/m³ for PM2.5, ppm for gaseous pollutants (Ozone, CO, NO2, SO2). This is the primary metric for national air quality
+       trend assessment and regulatory compliance evaluation.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: median_mean_value
-     type: double
-     description: National median of site-level annual means
-
+     type: DOUBLE
+     description: |
+       National median of site-level annual concentration averages, rounded to 4 decimal places. Provides a measure of central
+       tendency less sensitive to outlier monitoring sites than the arithmetic mean. Useful for understanding typical air quality
+       conditions while minimizing the influence of extremely high or low measurement sites that may skew the national average.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: p10_value
-     type: double
-     description: 10th percentile across all sites
-
+     type: DOUBLE
+     description: |
+       10th percentile of site-level annual concentration averages, rounded to 4 decimal places. Represents the concentration
+       level below which 10% of monitoring sites fall, effectively indicating the cleanest air quality conditions across the
+       national monitoring network. Useful for identifying best-case air quality scenarios and setting improvement targets.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: p90_value
-     type: double
-     description: 90th percentile across all sites
-
+     type: DOUBLE
+     description: |
+       90th percentile of site-level annual concentration averages, rounded to 4 decimal places. Represents the concentration
+       level above which only 10% of monitoring sites exceed, effectively indicating areas with the highest pollution burden.
+       Critical for identifying environmental justice concerns and prioritizing regulatory intervention in the most affected regions.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: site_count
-     type: integer
-     description: Number of monitoring sites reporting
-
+     type: BIGINT
+     description: |
+       Total number of distinct EPA AQS monitoring sites contributing data for the pollutant-year combination. Indicates the
+       statistical robustness of the national aggregates - higher site counts generally provide more reliable trend indicators.
+       Changes in site_count over time may reflect expansion or reduction of the monitoring network, which should be considered
+       when interpreting year-over-year trends.
+     checks:
+       - name: not_null
+       - name: positive
    - name: yoy_change_pct
-     type: double
-     description: Year-over-year % change in national average
+     type: DOUBLE
+     description: |-
+       Year-over-year percentage change in the national average concentration (avg_mean_value), rounded to 2 decimal places.
+       Calculated as ((current_year - previous_year) / previous_year) * 100. NULL for the first year of each pollutant (2019)
+       since no prior baseline exists. Negative values indicate air quality improvement (decreasing pollution), while positive
+       values suggest deterioration. This is the primary metric for assessing the effectiveness of air quality regulations and policies.

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

+91 additions, -23 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/marts/mart_pollutant_ranking.asset.sql

✓ Enhanced 'mart_pollutant_ranking.asset.sql'

Changes:
  /* @bruin

  name: marts.mart_pollutant_ranking
  type: duckdb.sql
+ description: |
+   Comprehensive air quality ranking analysis that ranks EPA monitoring sites by pollutant concentration levels both nationally and within each state for the five major criteria pollutants (PM2.5, Ozone, CO, NO2, SO2) from 2019-2023.
+
+   This mart provides critical insights into which geographic areas experience the highest pollution burden by creating dual ranking systems. National rankings identify the worst air quality sites across the entire United States, while state rankings enable within-state comparisons and identification of local pollution hotspots. Higher ranks indicate higher pollutant concentrations and worse air quality conditions.
+
+   The ranking methodology uses dense ranking based on annual mean concentrations, meaning sites with identical concentration values receive the same rank. This approach ensures fair comparison while handling cases where multiple monitoring sites may have exactly the same measured values due to rounding or measurement precision limitations.
+
+   Key business applications include environmental justice analysis (identifying disproportionately affected communities), regulatory enforcement prioritization, public health advisory targeting, and comparative performance assessment between states. The dual ranking structure enables both broad national policy insights and granular local environmental management decisions.
+
+   Data refresh occurs monthly as part of the EPA AQS pipeline, with rankings recalculated across the complete historical dataset to ensure consistency when new annual data becomes available. Each pollutant maintains separate ranking distributions due to different concentration scales and health impact thresholds.
+ tags:
+   - domain:environmental
+   - data_type:mart
+   - source:epa_aqs
+   - granularity:annual
+   - pipeline_role:mart
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - analysis_type:ranking_analysis
+   - scope:national_and_state
+   - update_pattern:snapshot
+   - use_case:pollution_hotspots
+
  materialization:
    type: table

  depends:
    - core.fct_measurements
    - core.dim_site

  columns:
    - name: site_id
-     type: varchar
-     description: Monitoring site identifier
+     type: VARCHAR
+     description: |
+       Foreign key reference to core.dim_site. EPA monitoring site identifier in composite format 'state_county_site'
+       using standardized EPA codes (e.g., '06_037_1103' for Los Angeles). Links to detailed geographic metadata for
+       spatial analysis and location-based filtering. Essential for mapping pollution hotspots and understanding
+       geographic distribution of air quality impacts.
      checks:
        - name: not_null
-
    - name: state_name
-     type: varchar
-     description: US state name
-
+     type: VARCHAR
+     description: |
+       Full US state name derived from EPA geographic references. Used for state-level aggregation and filtering in
+       analysis workflows. Enables state-to-state air quality comparisons and supports state-specific regulatory
+       reporting requirements. Standardized to official state names for consistency across the dataset.
+     checks:
+       - name: not_null
    - name: county_name
-     type: varchar
-     description: County name
-
+     type: VARCHAR
+     description: |
+       County name within the state where the monitoring site is located. Provides sub-state geographic granularity
+       for local air quality analysis. Critical for identifying county-level pollution patterns and supporting
+       local government environmental planning efforts. Some sites may have NULL county designations for special
+       monitoring locations.
    - name: pollutant
-     type: varchar
-     description: Pollutant short name
+     type: VARCHAR
+     description: |
+       Standardized EPA criteria pollutant identifier (PM2.5, Ozone, CO, NO2, SO2). These five pollutants represent
+       the major air quality indicators regulated under the Clean Air Act due to their significant public health impacts.
+       Rankings are calculated independently for each pollutant due to different concentration scales, health thresholds,
+       and regulatory standards.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of the air quality measurement (2019-2023 coverage). Rankings are calculated independently within
+       each year to account for temporal variations in air quality conditions, regulatory changes, and monitoring network
+       adjustments. Used for trend analysis to identify whether high-ranking sites consistently show poor air quality
+       over time.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - 2019
+           - 2020
+           - 2021
+           - 2022
+           - 2023
    - name: mean_value
-     type: double
-     description: Annual mean concentration
-
+     type: DOUBLE
+     description: |
+       Annual arithmetic mean pollutant concentration in standardized units, rounded to 4 decimal places. This is the
+       primary metric used for ranking calculations. Values represent site-level averages across all monitoring
+       instruments and observation periods during the year. Units vary by pollutant: µg/m³ for PM2.5, ppm for gaseous
+       pollutants (Ozone, CO, NO2, SO2). Higher values indicate worse air quality conditions.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: unit
-     type: varchar
-     description: Unit of measure
-
+     type: VARCHAR
+     description: |
+       Standardized EPA unit of measurement for the concentration value. Critical for proper interpretation of
+       rankings and concentration comparisons. 'Micrograms/cubic meter (LC)' for particulate matter (PM2.5),
+       'Parts per million' for gaseous pollutants. Units remain consistent within each pollutant type but vary
+       across different pollutant categories due to measurement methodology differences.
+     checks:
+       - name: not_null
    - name: national_rank
-     type: integer
-     description: Site rank nationally (1 = highest concentration) within pollutant+year
-
+     type: BIGINT
+     description: |
+       Dense rank of the monitoring site within all US sites for the specific pollutant-year combination, ordered by
+       descending concentration (rank 1 = highest concentration = worst air quality). Enables identification of the
+       most polluted locations nationally and supports federal environmental justice initiatives. Tied concentrations
+       receive identical ranks, with subsequent ranks continuing sequentially without gaps.
+     checks:
+       - name: not_null
+       - name: positive
    - name: state_rank
-     type: integer
-     description: Site rank within state for pollutant+year
+     type: BIGINT
+     description: |
+       Dense rank of the monitoring site within its state for the specific pollutant-year combination, ordered by
+       descending concentration (rank 1 = highest concentration within state = worst state-level air quality).
+       Enables state-level environmental management and supports local air quality improvement targeting.
+       Essential for intrastate comparisons and state regulatory prioritization efforts.
+     checks:
+       - name: not_null
+       - name: positive

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

+103 additions, -26 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/staging/stg_measurements.asset.sql

✓ Enhanced 'stg_measurements.asset.sql'

Changes:
  /* @bruin

  name: staging.stg_measurements
  type: duckdb.sql
+ description: |
+   Staging layer for EPA Air Quality System (AQS) annual concentration measurements, covering 2019-2023 data for five major criteria pollutants regulated under the Clean Air Act.
+
+   This table performs critical data normalization and quality filtering on raw EPA monitoring data:
+   - Standardizes pollutant names from verbose EPA parameter names to short codes (e.g., "PM2.5 - Local Conditions" → "PM2.5")
+   - Constructs composite site_id keys using EPA's hierarchical location coding (state_county_site format)
+   - Filters to exclude null, negative, or invalid concentration measurements
+   - Preserves Parameter Occurrence Code (POC) values for downstream aggregation across multiple monitoring instruments
+
+   The data represents annual arithmetic mean concentrations from EPA's continuous and manual monitoring networks. Multiple monitoring instruments (different POCs) may exist at a single site for the same pollutant, which are later aggregated in the core layer.
+
+   Downstream usage: Feeds both core.dim_site (unique monitoring locations) and core.fct_measurements (aggregated annual measurements) for air quality trend analysis, regulatory compliance reporting, and public health assessments.
+ tags:
+   - domain:environmental
+   - data_type:staging_table
+   - source:epa_aqs
+   - pipeline_role:staging
+   - granularity:site_pollutant_year_poc
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - update_pattern:replace
+   - quality:validated
+
  materialization:
    type: table

  depends:
    - raw.download_epa

  columns:
    - name: site_id
-     type: varchar
-     description: Unique site identifier (state_county_site)
+     type: VARCHAR
+     description: |
+       Composite primary key constructed as 'state_county_site' using EPA standardized location codes.
+       Format: zero-padded 2-digit state + 3-digit county + 4-digit site number (e.g., '06_037_1103').
+       Provides hierarchical geographic grouping and unique identification across the national monitoring network.
      checks:
        - name: not_null
-
    - name: state_name
-     type: varchar
-     description: US state name
-
+     type: VARCHAR
+     description: |
+       US state name as provided in EPA AQS data. Used for geographic analysis and state-level aggregations
+       in downstream mart tables. Trimmed to remove leading/trailing whitespace from source data.
    - name: county_name
-     type: varchar
-     description: County name
-
+     type: VARCHAR
+     description: |
+       County name within the state where the monitoring site is located. Essential for local air quality
+       assessments and county-level regulatory compliance reporting. May include special districts or parishes
+       depending on state administrative structure.
    - name: site_num
-     type: varchar
-     description: EPA site number
-
+     type: VARCHAR
+     description: |
+       EPA monitoring site number (4-digit, zero-padded). Unique within a state-county combination.
+       Represents the specific monitoring station location and remains consistent across years for
+       temporal trend analysis. Does not change if monitoring equipment is upgraded or replaced.
    - name: latitude
-     type: double
-     description: Site latitude
-
+     type: DOUBLE
+     description: |
+       Monitoring site latitude in decimal degrees (WGS84 datum). Used for spatial analysis, mapping,
+       and proximity calculations. May be null for sites with incomplete geographic metadata.
+       Coordinates can shift slightly over time if monitoring equipment is relocated within the same site designation.
    - name: longitude
-     type: double
-     description: Site longitude
-
+     type: DOUBLE
+     description: |
+       Monitoring site longitude in decimal degrees (WGS84 datum). Used for spatial analysis and geographic
+       visualization of air quality data. May be null for sites with incomplete geographic metadata.
+       Negative values represent western hemisphere locations (all US monitoring sites).
    - name: pollutant
-     type: varchar
-     description: Normalized pollutant short name
+     type: VARCHAR
+     description: |
+       Standardized pollutant identifier normalized from EPA's verbose parameter names to short codes.
+       Covers the five major criteria pollutants: PM2.5 (fine particulate matter), Ozone (ground-level ozone),
+       CO (carbon monoxide), NO2 (nitrogen dioxide), SO2 (sulfur dioxide). Used for consistent cross-pollutant
+       analysis and simplified mart table design.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: parameter_name
-     type: varchar
-     description: Original EPA parameter name
-
+     type: VARCHAR
+     description: |
+       Original EPA parameter name from source AQS data (e.g., 'PM2.5 - Local Conditions', 'Carbon monoxide').
+       Preserved for traceability back to EPA's official parameter definitions and regulatory documentation.
+       Essential for data lineage auditing and ensuring compliance with EPA reporting standards.
    - name: mean_value
-     type: double
-     description: Annual arithmetic mean concentration
+     type: DOUBLE
+     description: |
+       Annual arithmetic mean concentration for the pollutant at this monitoring site. Represents the primary
+       air quality metric used for regulatory compliance and health impact assessments. Units vary by pollutant
+       (µg/m³ for PM2.5, ppm for gases). Values are filtered to exclude nulls and negatives during staging.
      checks:
        - name: not_null
-
+       - name: non_negative
    - name: unit
-     type: varchar
-     description: Unit of measure
-
+     type: VARCHAR
+     description: |
+       Unit of measurement for the concentration value. Standardized EPA units include 'Micrograms/cubic meter (LC)'
+       for particulate matter (PM2.5) and 'Parts per million' for gaseous pollutants (Ozone, CO, NO2, SO2).
+       Critical for proper interpretation and comparison of concentration levels across different pollutant types.
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of the annual measurement (2019-2023 range). Represents the EPA AQS annual reporting
+       period used for regulatory compliance monitoring. Essential dimension for temporal trend analysis
+       and year-over-year air quality comparisons.
      checks:
        - name: not_null
+   - name: poc
+     type: INTEGER
+     description: |-
+       Parameter Occurrence Code - EPA's identifier for distinguishing between multiple monitoring instruments
+       measuring the same pollutant at a single site. Integer values (1, 2, 3, etc.) represent different
+       monitoring methods, instruments, or sampling frequencies. Multiple POCs are later aggregated in
+       core.fct_measurements to provide unified site-level concentrations.

  @bruin */

  SELECT
      -- Composite site key
      LPAD(CAST("State Code" AS VARCHAR), 2, '0')
          || '_' || LPAD(CAST("County Code" AS VARCHAR), 3, '0')
          || '_' || LPAD(CAST("Site Num" AS VARCHAR), 4, '0')               AS site_id,

      TRIM("State Name")                                                      AS state_name,
      TRIM("County Name")                                                     AS county_name,
      LPAD(CAST("Site Num" AS VARCHAR), 4, '0')                              AS site_num,

      TRY_CAST("Latitude"  AS DOUBLE)                                        AS latitude,
      TRY_CAST("Longitude" AS DOUBLE)                                        AS longitude,

      -- Normalize pollutant names to short labels
      CASE TRIM("Parameter Name")
          WHEN 'PM2.5 - Local Conditions' THEN 'PM2.5'
          WHEN 'Ozone'                    THEN 'Ozone'
          WHEN 'Carbon monoxide'          THEN 'CO'
          WHEN 'Nitrogen dioxide (NO2)'   THEN 'NO2'
          WHEN 'Sulfur dioxide'           THEN 'SO2'
          ELSE TRIM("Parameter Name")
      END                                                                     AS pollutant,

      TRIM("Parameter Name")                                                  AS parameter_name,

      TRY_CAST("Arithmetic Mean" AS DOUBLE)                                  AS mean_value,
      TRIM("Units of Measure")                                               AS unit,
      CAST("Year" AS INTEGER)                                                AS year,
      CAST("POC" AS INTEGER)                                                 AS poc

  FROM read_csv_auto('epa-air-quality/data/epa_combined.csv', header=true, ignore_errors=true)

  WHERE
      "Arithmetic Mean" IS NOT NULL
      AND TRY_CAST("Arithmetic Mean" AS DOUBLE) >= 0
      AND "Year" IS NOT NULL
      AND "Parameter Name" IN (
          'PM2.5 - Local Conditions',
          'Ozone',
          'Carbon monoxide',
          'Nitrogen dioxide (NO2)',
          'Sulfur dioxide'
      )

+93 additions, -32 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/core/dim_site.asset.sql

✓ Enhanced 'dim_site.asset.sql'

Changes:
  /* @bruin

  name: core.dim_site
  type: duckdb.sql
+ description: |
+   EPA Air Quality System (AQS) monitoring site dimension table containing unique geographic and administrative metadata for air quality monitoring stations across the United States (2019-2023 period).
+
+   This dimension table provides one record per unique monitoring site, serving as the geographic foundation for air quality analysis across multiple pollutants and years. Sites represent physical monitoring station locations within EPA's national ambient air monitoring network, each equipped to measure one or more criteria pollutants (PM2.5, Ozone, CO, NO2, SO2).
+
+   The table aggregates site information from time-series measurement data, using the most recent non-null coordinates for each site to handle cases where monitoring equipment may have been relocated within the same site designation. This approach ensures coordinate stability for spatial analysis while preserving the EPA's hierarchical location coding system.
+
+   Key transformations: Deduplicates sites across multiple years of measurements, resolves coordinate conflicts by prioritizing most recent valid coordinates, maintains EPA's standardized site identification scheme for consistent cross-referencing.
+
+   Downstream usage: Joined with core.fct_measurements for geographic analysis, feeds state-level aggregations in mart tables, enables spatial clustering and proximity analysis, supports regulatory compliance reporting by administrative boundaries.
+
+   Data lineage: Raw EPA AQS CSV files → staging.stg_measurements (normalization) → core.dim_site (deduplication + coordinate resolution).
+
+   Business context: Essential for environmental compliance monitoring, public health impact assessments, air quality trend analysis by geographic region, and policy development targeting specific states or counties with poor air quality.
+ tags:
+   - domain:environmental
+   - data_type:dimension_table
+   - source:epa_aqs
+   - pipeline_role:core
+   - granularity:monitoring_site
+   - geographic_scope:us_national
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - quality:validated
+   - update_pattern:replace
+   - regulatory:clean_air_act
+
  materialization:
    type: table

  depends:
    - staging.stg_measurements

  columns:
    - name: site_id
-     type: varchar
-     description: Surrogate site key (state_county_site)
+     type: VARCHAR
+     description: |
+       Composite primary key constructed as 'state_county_site' using EPA standardized location codes.
+       Format: zero-padded 2-digit state code + underscore + 3-digit county code + underscore + 4-digit site number
+       (e.g., '06_037_1103' for a Los Angeles County site). Provides hierarchical geographic grouping
+       enabling rollups to county and state levels while maintaining unique site identification across
+       the national monitoring network. This key remains stable across years even if monitoring equipment is upgraded.
      checks:
        - name: not_null
        - name: unique
-
    - name: state_name
-     type: varchar
-     description: US state name
+     type: VARCHAR
+     description: |
+       Official US state name as recorded in EPA AQS data (e.g., 'California', 'Texas', 'New York').
+       Includes all 50 states plus District of Columbia and US territories with active monitoring sites.
+       Critical dimension for state-level environmental policy analysis, regulatory compliance reporting,
+       and cross-state air quality comparisons. Trimmed of whitespace during staging for data consistency.
      checks:
        - name: not_null
-
    - name: county_name
-     type: varchar
-     description: County name
-
+     type: VARCHAR
+     description: |
+       County or equivalent administrative subdivision name within the state where the monitoring site is located.
+       Essential for local air quality assessments, county-level regulatory compliance reporting under the Clean Air Act,
+       and identifying non-attainment areas. May include parishes (Louisiana), boroughs (Alaska), or special districts
+       depending on state administrative structure. Used extensively in mart tables for sub-state geographic analysis.
    - name: site_num
-     type: varchar
-     description: EPA monitoring site number
-
+     type: VARCHAR
+     description: |
+       EPA monitoring site number as a 4-digit, zero-padded string (e.g., '0023', '1103').
+       Unique within each state-county combination and represents the specific monitoring station location.
+       This identifier remains consistent across years for temporal trend analysis, enabling tracking
+       of long-term air quality changes at specific geographic points. Does not change when monitoring
+       equipment is upgraded or replaced at the same location.
    - name: latitude
-     type: double
-     description: Site latitude
-
+     type: DOUBLE
+     description: |
+       Monitoring site latitude in decimal degrees using WGS84 datum (North American standard).
+       Used for spatial analysis, distance calculations, and geographic visualization of air quality data.
+       Values range approximately from 18°N (southern Florida/Hawaii) to 71°N (northern Alaska).
+       Derived using most recent non-null coordinates per site to handle equipment relocations while
+       preserving spatial analysis capability. May be null for sites with incomplete EPA geographic metadata.
    - name: longitude
-     type: double
-     description: Site longitude
+     type: DOUBLE
+     description: |-
+       Monitoring site longitude in decimal degrees using WGS84 datum. All values are negative representing
+       western hemisphere locations (-67°W to -180°W covering continental US, Alaska, Hawaii, and territories).
+       Critical for mapping applications, spatial clustering analysis, and proximity-based air quality studies.
+       Derived using most recent non-null coordinates per site to ensure data quality for downstream geographic analysis.
+       May be null for sites with incomplete EPA geographic metadata.

  @bruin */
mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/core/fct_measurements.asset.sql

✓ Enhanced 'fct_measurements.asset.sql'

Changes:
  /* @bruin

  name: core.fct_measurements
  type: duckdb.sql
+ description: |
+   Central fact table containing aggregated EPA Air Quality System (AQS) measurements for the five major criteria pollutants monitored across the United States from 2019-2023.
+
+   This table represents the core analytical layer that aggregates raw monitoring data to one record per site-pollutant-year combination. When multiple monitoring instruments (POCs) exist at a single site for the same pollutant, the mean_value is averaged across all instruments to provide a unified annual concentration for analytical purposes.
+
+   The data undergoes quality filtering to exclude null, negative, or invalid measurements, ensuring all values represent legitimate air quality readings. The table serves as the primary source for national air quality trend analysis, state comparisons, and pollutant ranking reports in downstream mart tables.
+
+   Key business insights: Provides foundation for understanding long-term air quality trends, regulatory compliance monitoring, and public health impact assessments across different geographic regions and pollutant types.
+ tags:
+   - domain:environmental
+   - data_type:fact_table
+   - source:epa_aqs
+   - granularity:annual
+   - pipeline_role:core
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - quality:high_governance

  materialization:
    type: table

  depends:
    - staging.stg_measurements
    - core.dim_site

  columns:
    - name: measurement_id
      type: VARCHAR
-     description: Surrogate key (site_id + pollutant + year)
+     description: |
+       Surrogate primary key constructed as site_id || '__' || pollutant || '__' || year.
+       Unique identifier ensuring one record per monitoring site-pollutant-year combination.
+       Example format: '01_073_0023__PM2.5__2023'
      checks:
        - name: not_null
        - name: unique
    - name: site_id
      type: VARCHAR
-     description: Foreign key to core.dim_site
+     description: |
+       Foreign key reference to core.dim_site. Composite identifier in format 'state_county_site'
+       using EPA standardized codes (e.g., '06_037_1103' for Los Angeles). Links to geographic
+       and location metadata for spatial analysis.
      checks:
        - name: not_null
    - name: pollutant
      type: VARCHAR
-     description: Normalized pollutant short name
+     description: |
+       Standardized pollutant identifier for the five major criteria air pollutants regulated
+       under the Clean Air Act. Normalized from verbose EPA parameter names to short codes
+       for consistent analysis across different monitoring programs.
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
-     description: Measurement year
+     description: |
+       Calendar year of the measurement (2019-2023 range). Represents the annual reporting
+       period for EPA AQS data. Used for temporal trend analysis and year-over-year comparisons.
      checks:
        - name: not_null
    - name: mean_value
      type: DOUBLE
-     description: Annual arithmetic mean concentration
+     description: |
+       Annual arithmetic mean concentration averaged across all monitoring instruments (POCs)
+       at the site for the given pollutant-year combination. Represents the primary air quality
+       metric used for regulatory compliance and health impact assessments.
+       Units vary by pollutant: µg/m³ for particulates, ppm for gases.
      checks:
        - name: not_null
        - name: non_negative
    - name: unit
      type: VARCHAR
-     description: Unit of measure
+     description: |
+       Unit of measurement for the concentration value. Standardized EPA units:
+       'Micrograms/cubic meter (LC)' for PM2.5, 'Parts per million' for gaseous pollutants.
+       Critical for proper interpretation and comparison of concentration levels across pollutants.
    - name: parameter_name
      type: VARCHAR
+     description: |-
+       Original EPA parameter name from source data (e.g., 'PM2.5 - Local Conditions',
+       'Carbon monoxide'). Preserved for traceability back to EPA AQS official parameter
+       definitions and regulatory reference documentation.

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

+45 additions, -6 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/marts/mart_state_comparison.asset.sql

✓ Enhanced 'mart_state_comparison.asset.sql'

Changes:
  /* @bruin

  name: marts.mart_state_comparison
  type: duckdb.sql
+ description: |
+   State-level air quality analysis table comparing each US state's pollution performance against national averages for EPA criteria pollutants (2019-2023).
+
+   This mart aggregates monitoring site data to the state level, providing key metrics for environmental policy analysis, public health assessments, and regulatory compliance tracking. Each row represents one state-pollutant-year combination with comparative statistics.
+
+   The table enables identification of the most and least polluted states, tracks state-level air quality trends over time, and quantifies how far each state deviates from national pollution averages. State rankings are calculated within each pollutant-year combination to facilitate cross-state comparisons.
+
+   Key use cases: State environmental scorecards, regulatory compliance monitoring, public health impact assessments, policy effectiveness evaluation, and identifying states requiring targeted air quality interventions.
+
+   Data quality notes: States with fewer than one monitoring site for a given pollutant-year may show less reliable averages. Alaska and Hawaii may have limited monitoring coverage compared to continental US states.
+ tags:
+   - domain:environmental
+   - data_type:mart_table
+   - source:epa_aqs
+   - granularity:state_annual
+   - pipeline_role:mart
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - analytics_use:comparative_analysis
+   - governance:regulatory_compliance
+   - geographic_scope:us_states
+
  materialization:
    type: table

  depends:
    - core.fct_measurements
    - core.dim_site

  columns:
    - name: state_name
-     type: varchar
-     description: US state name
+     type: VARCHAR
+     description: |
+       Official US state name (e.g., 'California', 'Texas'). All 50 states plus DC and territories
+       with EPA monitoring sites included. Used for geographic filtering and reporting breakdowns.
      checks:
        - name: not_null
-
    - name: pollutant
-     type: varchar
-     description: Pollutant short name
+     type: VARCHAR
+     description: |
+       EPA criteria pollutant identifier standardized to short codes. One of five major air quality
+       indicators regulated under the Clean Air Act: PM2.5 (fine particulate matter), Ozone (ground-level),
+       CO (carbon monoxide), NO2 (nitrogen dioxide), SO2 (sulfur dioxide). Critical for pollutant-specific analysis.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of measurements (2019-2023 range). Represents the annual EPA AQS reporting period.
+       Used for temporal trend analysis and year-over-year state performance comparisons.
      checks:
        - name: not_null
-
    - name: avg_mean_value
-     type: double
-     description: State average of site-level annual means
-
+     type: DOUBLE
+     description: |
+       State-level arithmetic mean of all monitoring sites' annual average concentrations within the state.
+       Represents the typical air quality across the state for the given pollutant-year. Units vary by
+       pollutant (µg/m³ for PM2.5, ppm for gases). This is the primary metric for state air quality assessment.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: max_mean_value
-     type: double
-     description: Worst site reading in the state
-
+     type: DOUBLE
+     description: |
+       Highest annual average concentration recorded at any monitoring site within the state for the
+       given pollutant-year. Identifies pollution hotspots and worst-case air quality conditions within
+       each state. Critical for understanding intra-state pollution variability.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: site_count
-     type: integer
-     description: Number of monitoring sites in the state
-
+     type: BIGINT
+     description: |
+       Number of EPA monitoring sites reporting valid annual data within the state for the specific
+       pollutant-year combination. Indicates data reliability and geographic coverage density.
+       Higher counts suggest more representative state averages.
+     checks:
+       - name: not_null
+       - name: positive
    - name: national_avg
-     type: double
-     description: National average for the same pollutant and year
-
+     type: DOUBLE
+     description: |
+       National arithmetic mean concentration for the same pollutant-year combination across all US
+       monitoring sites. Baseline metric for comparative analysis enabling identification of states
+       performing better or worse than the national average.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: pct_above_national
-     type: double
-     description: How many % above/below the national average
-
+     type: DOUBLE
+     description: |
+       Percentage deviation of state average from national average ((state_avg - national_avg) / national_avg * 100).
+       Positive values indicate above-national pollution levels, negative values indicate below-national levels.
+       Key metric for identifying environmental leaders and laggards among states.
    - name: state_rank
-     type: integer
-     description: State rank (1 = most polluted) within pollutant+year
+     type: BIGINT
+     description: |
+       State ranking within each pollutant-year combination ordered by avg_mean_value (1 = highest pollution,
+       50+ = lowest pollution). Enables easy identification of most and least polluted states for each
+       pollutant-year. Essential for state-to-state competitive analysis and policy benchmarking.
+     checks:
+       - name: not_null
+       - name: positive

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

+87 additions, -26 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/marts/mart_aqi_annual_trends.asset.sql

✓ Enhanced 'mart_aqi_annual_trends.asset.sql'

Changes:
  /* @bruin

  name: marts.mart_aqi_annual_trends
  type: duckdb.sql
+ description: |
+   National-level annual air quality trends summary for the five major EPA criteria pollutants (PM2.5, Ozone, CO, NO2, SO2) from 2019-2023.
+
+   This mart aggregates site-level measurements to provide a national perspective on air quality patterns, calculating key statistical measures including central tendencies and distribution percentiles. The table enables trend analysis through year-over-year percentage change calculations, making it the primary resource for understanding long-term national air quality improvements or deterioration.
+
+   Each row represents one pollutant-year combination with national statistics computed across all EPA AQS monitoring sites. The year-over-year change metric uses the previous year as baseline, so 2019 data will have NULL yoy_change_pct values. Statistical measures are rounded to 4 decimal places for concentration values and 2 decimal places for percentage changes to balance precision with readability.
+
+   Primary use cases include regulatory compliance reporting, public health trend assessment, environmental policy impact evaluation, and comparative analysis across pollutant types. The table serves data consumers ranging from EPA analysts to environmental researchers and public health officials.
+ tags:
+   - domain:environmental
+   - data_type:mart
+   - source:epa_aqs
+   - granularity:annual
+   - pipeline_role:mart
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - analysis_type:trend_analysis
+   - scope:national
+   - update_pattern:append_only
+
  materialization:
    type: table

  depends:
    - core.fct_measurements

  columns:
    - name: pollutant
-     type: varchar
-     description: Pollutant short name
+     type: VARCHAR
+     description: |
+       Standardized pollutant identifier (PM2.5, Ozone, CO, NO2, SO2). These represent the five major criteria air pollutants
+       regulated under the Clean Air Act. Normalized from verbose EPA parameter names to enable consistent analysis and reporting
+       across different monitoring programs and time periods.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of the aggregated measurements (2019-2023 coverage). Represents the EPA AQS annual reporting period.
+       Used as the temporal dimension for trend analysis and year-over-year comparison calculations.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - 2019
+           - 2020
+           - 2021
+           - 2022
+           - 2023
    - name: avg_mean_value
-     type: double
-     description: National average of site-level annual means
-
+     type: DOUBLE
+     description: |
+       National arithmetic mean of site-level annual concentration averages, rounded to 4 decimal places. Computed by averaging
+       the mean_value from all EPA monitoring sites for the given pollutant-year combination. Units vary by pollutant:
+       µg/m³ for PM2.5, ppm for gaseous pollutants (Ozone, CO, NO2, SO2). This is the primary metric for national air quality
+       trend assessment and regulatory compliance evaluation.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: median_mean_value
-     type: double
-     description: National median of site-level annual means
-
+     type: DOUBLE
+     description: |
+       National median of site-level annual concentration averages, rounded to 4 decimal places. Provides a measure of central
+       tendency less sensitive to outlier monitoring sites than the arithmetic mean. Useful for understanding typical air quality
+       conditions while minimizing the influence of extremely high or low measurement sites that may skew the national average.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: p10_value
-     type: double
-     description: 10th percentile across all sites
-
+     type: DOUBLE
+     description: |
+       10th percentile of site-level annual concentration averages, rounded to 4 decimal places. Represents the concentration
+       level below which 10% of monitoring sites fall, effectively indicating the cleanest air quality conditions across the
+       national monitoring network. Useful for identifying best-case air quality scenarios and setting improvement targets.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: p90_value
-     type: double
-     description: 90th percentile across all sites
-
+     type: DOUBLE
+     description: |
+       90th percentile of site-level annual concentration averages, rounded to 4 decimal places. Represents the concentration
+       level above which only 10% of monitoring sites exceed, effectively indicating areas with the highest pollution burden.
+       Critical for identifying environmental justice concerns and prioritizing regulatory intervention in the most affected regions.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: site_count
-     type: integer
-     description: Number of monitoring sites reporting
-
+     type: BIGINT
+     description: |
+       Total number of distinct EPA AQS monitoring sites contributing data for the pollutant-year combination. Indicates the
+       statistical robustness of the national aggregates - higher site counts generally provide more reliable trend indicators.
+       Changes in site_count over time may reflect expansion or reduction of the monitoring network, which should be considered
+       when interpreting year-over-year trends.
+     checks:
+       - name: not_null
+       - name: positive
    - name: yoy_change_pct
-     type: double
-     description: Year-over-year % change in national average
+     type: DOUBLE
+     description: |-
+       Year-over-year percentage change in the national average concentration (avg_mean_value), rounded to 2 decimal places.
+       Calculated as ((current_year - previous_year) / previous_year) * 100. NULL for the first year of each pollutant (2019)
+       since no prior baseline exists. Negative values indicate air quality improvement (decreasing pollution), while positive
+       values suggest deterioration. This is the primary metric for assessing the effectiveness of air quality regulations and policies.

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

+91 additions, -23 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/marts/mart_pollutant_ranking.asset.sql

✓ Enhanced 'mart_pollutant_ranking.asset.sql'

Changes:
  /* @bruin

  name: marts.mart_pollutant_ranking
  type: duckdb.sql
+ description: |
+   Comprehensive air quality ranking analysis that ranks EPA monitoring sites by pollutant concentration levels both nationally and within each state for the five major criteria pollutants (PM2.5, Ozone, CO, NO2, SO2) from 2019-2023.
+
+   This mart provides critical insights into which geographic areas experience the highest pollution burden by creating dual ranking systems. National rankings identify the worst air quality sites across the entire United States, while state rankings enable within-state comparisons and identification of local pollution hotspots. Higher ranks indicate higher pollutant concentrations and worse air quality conditions.
+
+   The ranking methodology uses dense ranking based on annual mean concentrations, meaning sites with identical concentration values receive the same rank. This approach ensures fair comparison while handling cases where multiple monitoring sites may have exactly the same measured values due to rounding or measurement precision limitations.
+
+   Key business applications include environmental justice analysis (identifying disproportionately affected communities), regulatory enforcement prioritization, public health advisory targeting, and comparative performance assessment between states. The dual ranking structure enables both broad national policy insights and granular local environmental management decisions.
+
+   Data refresh occurs monthly as part of the EPA AQS pipeline, with rankings recalculated across the complete historical dataset to ensure consistency when new annual data becomes available. Each pollutant maintains separate ranking distributions due to different concentration scales and health impact thresholds.
+ tags:
+   - domain:environmental
+   - data_type:mart
+   - source:epa_aqs
+   - granularity:annual
+   - pipeline_role:mart
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - analysis_type:ranking_analysis
+   - scope:national_and_state
+   - update_pattern:snapshot
+   - use_case:pollution_hotspots
+
  materialization:
    type: table

  depends:
    - core.fct_measurements
    - core.dim_site

  columns:
    - name: site_id
-     type: varchar
-     description: Monitoring site identifier
+     type: VARCHAR
+     description: |
+       Foreign key reference to core.dim_site. EPA monitoring site identifier in composite format 'state_county_site'
+       using standardized EPA codes (e.g., '06_037_1103' for Los Angeles). Links to detailed geographic metadata for
+       spatial analysis and location-based filtering. Essential for mapping pollution hotspots and understanding
+       geographic distribution of air quality impacts.
      checks:
        - name: not_null
-
    - name: state_name
-     type: varchar
-     description: US state name
-
+     type: VARCHAR
+     description: |
+       Full US state name derived from EPA geographic references. Used for state-level aggregation and filtering in
+       analysis workflows. Enables state-to-state air quality comparisons and supports state-specific regulatory
+       reporting requirements. Standardized to official state names for consistency across the dataset.
+     checks:
+       - name: not_null
    - name: county_name
-     type: varchar
-     description: County name
-
+     type: VARCHAR
+     description: |
+       County name within the state where the monitoring site is located. Provides sub-state geographic granularity
+       for local air quality analysis. Critical for identifying county-level pollution patterns and supporting
+       local government environmental planning efforts. Some sites may have NULL county designations for special
+       monitoring locations.
    - name: pollutant
-     type: varchar
-     description: Pollutant short name
+     type: VARCHAR
+     description: |
+       Standardized EPA criteria pollutant identifier (PM2.5, Ozone, CO, NO2, SO2). These five pollutants represent
+       the major air quality indicators regulated under the Clean Air Act due to their significant public health impacts.
+       Rankings are calculated independently for each pollutant due to different concentration scales, health thresholds,
+       and regulatory standards.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of the air quality measurement (2019-2023 coverage). Rankings are calculated independently within
+       each year to account for temporal variations in air quality conditions, regulatory changes, and monitoring network
+       adjustments. Used for trend analysis to identify whether high-ranking sites consistently show poor air quality
+       over time.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - 2019
+           - 2020
+           - 2021
+           - 2022
+           - 2023
    - name: mean_value
-     type: double
-     description: Annual mean concentration
-
+     type: DOUBLE
+     description: |
+       Annual arithmetic mean pollutant concentration in standardized units, rounded to 4 decimal places. This is the
+       primary metric used for ranking calculations. Values represent site-level averages across all monitoring
+       instruments and observation periods during the year. Units vary by pollutant: µg/m³ for PM2.5, ppm for gaseous
+       pollutants (Ozone, CO, NO2, SO2). Higher values indicate worse air quality conditions.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: unit
-     type: varchar
-     description: Unit of measure
-
+     type: VARCHAR
+     description: |
+       Standardized EPA unit of measurement for the concentration value. Critical for proper interpretation of
+       rankings and concentration comparisons. 'Micrograms/cubic meter (LC)' for particulate matter (PM2.5),
+       'Parts per million' for gaseous pollutants. Units remain consistent within each pollutant type but vary
+       across different pollutant categories due to measurement methodology differences.
+     checks:
+       - name: not_null
    - name: national_rank
-     type: integer
-     description: Site rank nationally (1 = highest concentration) within pollutant+year
-
+     type: BIGINT
+     description: |
+       Dense rank of the monitoring site within all US sites for the specific pollutant-year combination, ordered by
+       descending concentration (rank 1 = highest concentration = worst air quality). Enables identification of the
+       most polluted locations nationally and supports federal environmental justice initiatives. Tied concentrations
+       receive identical ranks, with subsequent ranks continuing sequentially without gaps.
+     checks:
+       - name: not_null
+       - name: positive
    - name: state_rank
-     type: integer
-     description: Site rank within state for pollutant+year
+     type: BIGINT
+     description: |
+       Dense rank of the monitoring site within its state for the specific pollutant-year combination, ordered by
+       descending concentration (rank 1 = highest concentration within state = worst state-level air quality).
+       Enables state-level environmental management and supports local air quality improvement targeting.
+       Essential for intrastate comparisons and state regulatory prioritization efforts.
+     checks:
+       - name: not_null
+       - name: positive

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

+103 additions, -26 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/staging/stg_measurements.asset.sql

✓ Enhanced 'stg_measurements.asset.sql'

Changes:
  /* @bruin

  name: staging.stg_measurements
  type: duckdb.sql
+ description: |
+   Staging layer for EPA Air Quality System (AQS) annual concentration measurements, covering 2019-2023 data for five major criteria pollutants regulated under the Clean Air Act.
+
+   This table performs critical data normalization and quality filtering on raw EPA monitoring data:
+   - Standardizes pollutant names from verbose EPA parameter names to short codes (e.g., "PM2.5 - Local Conditions" → "PM2.5")
+   - Constructs composite site_id keys using EPA's hierarchical location coding (state_county_site format)
+   - Filters to exclude null, negative, or invalid concentration measurements
+   - Preserves Parameter Occurrence Code (POC) values for downstream aggregation across multiple monitoring instruments
+
+   The data represents annual arithmetic mean concentrations from EPA's continuous and manual monitoring networks. Multiple monitoring instruments (different POCs) may exist at a single site for the same pollutant, which are later aggregated in the core layer.
+
+   Downstream usage: Feeds both core.dim_site (unique monitoring locations) and core.fct_measurements (aggregated annual measurements) for air quality trend analysis, regulatory compliance reporting, and public health assessments.
+ tags:
+   - domain:environmental
+   - data_type:staging_table
+   - source:epa_aqs
+   - pipeline_role:staging
+   - granularity:site_pollutant_year_poc
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - update_pattern:replace
+   - quality:validated
+
  materialization:
    type: table

  depends:
    - raw.download_epa

  columns:
    - name: site_id
-     type: varchar
-     description: Unique site identifier (state_county_site)
+     type: VARCHAR
+     description: |
+       Composite primary key constructed as 'state_county_site' using EPA standardized location codes.
+       Format: zero-padded 2-digit state + 3-digit county + 4-digit site number (e.g., '06_037_1103').
+       Provides hierarchical geographic grouping and unique identification across the national monitoring network.
      checks:
        - name: not_null
-
    - name: state_name
-     type: varchar
-     description: US state name
-
+     type: VARCHAR
+     description: |
+       US state name as provided in EPA AQS data. Used for geographic analysis and state-level aggregations
+       in downstream mart tables. Trimmed to remove leading/trailing whitespace from source data.
    - name: county_name
-     type: varchar
-     description: County name
-
+     type: VARCHAR
+     description: |
+       County name within the state where the monitoring site is located. Essential for local air quality
+       assessments and county-level regulatory compliance reporting. May include special districts or parishes
+       depending on state administrative structure.
    - name: site_num
-     type: varchar
-     description: EPA site number
-
+     type: VARCHAR
+     description: |
+       EPA monitoring site number (4-digit, zero-padded). Unique within a state-county combination.
+       Represents the specific monitoring station location and remains consistent across years for
+       temporal trend analysis. Does not change if monitoring equipment is upgraded or replaced.
    - name: latitude
-     type: double
-     description: Site latitude
-
+     type: DOUBLE
+     description: |
+       Monitoring site latitude in decimal degrees (WGS84 datum). Used for spatial analysis, mapping,
+       and proximity calculations. May be null for sites with incomplete geographic metadata.
+       Coordinates can shift slightly over time if monitoring equipment is relocated within the same site designation.
    - name: longitude
-     type: double
-     description: Site longitude
-
+     type: DOUBLE
+     description: |
+       Monitoring site longitude in decimal degrees (WGS84 datum). Used for spatial analysis and geographic
+       visualization of air quality data. May be null for sites with incomplete geographic metadata.
+       Negative values represent western hemisphere locations (all US monitoring sites).
    - name: pollutant
-     type: varchar
-     description: Normalized pollutant short name
+     type: VARCHAR
+     description: |
+       Standardized pollutant identifier normalized from EPA's verbose parameter names to short codes.
+       Covers the five major criteria pollutants: PM2.5 (fine particulate matter), Ozone (ground-level ozone),
+       CO (carbon monoxide), NO2 (nitrogen dioxide), SO2 (sulfur dioxide). Used for consistent cross-pollutant
+       analysis and simplified mart table design.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: parameter_name
-     type: varchar
-     description: Original EPA parameter name
-
+     type: VARCHAR
+     description: |
+       Original EPA parameter name from source AQS data (e.g., 'PM2.5 - Local Conditions', 'Carbon monoxide').
+       Preserved for traceability back to EPA's official parameter definitions and regulatory documentation.
+       Essential for data lineage auditing and ensuring compliance with EPA reporting standards.
    - name: mean_value
-     type: double
-     description: Annual arithmetic mean concentration
+     type: DOUBLE
+     description: |
+       Annual arithmetic mean concentration for the pollutant at this monitoring site. Represents the primary
+       air quality metric used for regulatory compliance and health impact assessments. Units vary by pollutant
+       (µg/m³ for PM2.5, ppm for gases). Values are filtered to exclude nulls and negatives during staging.
      checks:
        - name: not_null
-
+       - name: non_negative
    - name: unit
-     type: varchar
-     description: Unit of measure
-
+     type: VARCHAR
+     description: |
+       Unit of measurement for the concentration value. Standardized EPA units include 'Micrograms/cubic meter (LC)'
+       for particulate matter (PM2.5) and 'Parts per million' for gaseous pollutants (Ozone, CO, NO2, SO2).
+       Critical for proper interpretation and comparison of concentration levels across different pollutant types.
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of the annual measurement (2019-2023 range). Represents the EPA AQS annual reporting
+       period used for regulatory compliance monitoring. Essential dimension for temporal trend analysis
+       and year-over-year air quality comparisons.
      checks:
        - name: not_null
+   - name: poc
+     type: INTEGER
+     description: |-
+       Parameter Occurrence Code - EPA's identifier for distinguishing between multiple monitoring instruments
+       measuring the same pollutant at a single site. Integer values (1, 2, 3, etc.) represent different
+       monitoring methods, instruments, or sampling frequencies. Multiple POCs are later aggregated in
+       core.fct_measurements to provide unified site-level concentrations.

  @bruin */

  SELECT
      -- Composite site key
      LPAD(CAST("State Code" AS VARCHAR), 2, '0')
          || '_' || LPAD(CAST("County Code" AS VARCHAR), 3, '0')
          || '_' || LPAD(CAST("Site Num" AS VARCHAR), 4, '0')               AS site_id,

      TRIM("State Name")                                                      AS state_name,
      TRIM("County Name")                                                     AS county_name,
      LPAD(CAST("Site Num" AS VARCHAR), 4, '0')                              AS site_num,

      TRY_CAST("Latitude"  AS DOUBLE)                                        AS latitude,
      TRY_CAST("Longitude" AS DOUBLE)                                        AS longitude,

      -- Normalize pollutant names to short labels
      CASE TRIM("Parameter Name")
          WHEN 'PM2.5 - Local Conditions' THEN 'PM2.5'
          WHEN 'Ozone'                    THEN 'Ozone'
          WHEN 'Carbon monoxide'          THEN 'CO'
          WHEN 'Nitrogen dioxide (NO2)'   THEN 'NO2'
          WHEN 'Sulfur dioxide'           THEN 'SO2'
          ELSE TRIM("Parameter Name")
      END                                                                     AS pollutant,

      TRIM("Parameter Name")                                                  AS parameter_name,

      TRY_CAST("Arithmetic Mean" AS DOUBLE)                                  AS mean_value,
      TRIM("Units of Measure")                                               AS unit,
      CAST("Year" AS INTEGER)                                                AS year,
      CAST("POC" AS INTEGER)                                                 AS poc

  FROM read_csv_auto('epa-air-quality/data/epa_combined.csv', header=true, ignore_errors=true)

  WHERE
      "Arithmetic Mean" IS NOT NULL
      AND TRY_CAST("Arithmetic Mean" AS DOUBLE) >= 0
      AND "Year" IS NOT NULL
      AND "Parameter Name" IN (
          'PM2.5 - Local Conditions',
          'Ozone',
          'Carbon monoxide',
          'Nitrogen dioxide (NO2)',
          'Sulfur dioxide'
      )

+93 additions, -32 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/core/dim_site.asset.sql

✓ Enhanced 'dim_site.asset.sql'

Changes:
  /* @bruin

  name: core.dim_site
  type: duckdb.sql
+ description: |
+   EPA Air Quality System (AQS) monitoring site dimension table containing unique geographic and administrative metadata for air quality monitoring stations across the United States (2019-2023 period).
+
+   This dimension table provides one record per unique monitoring site, serving as the geographic foundation for air quality analysis across multiple pollutants and years. Sites represent physical monitoring station locations within EPA's national ambient air monitoring network, each equipped to measure one or more criteria pollutants (PM2.5, Ozone, CO, NO2, SO2).
+
+   The table aggregates site information from time-series measurement data, using the most recent non-null coordinates for each site to handle cases where monitoring equipment may have been relocated within the same site designation. This approach ensures coordinate stability for spatial analysis while preserving the EPA's hierarchical location coding system.
+
+   Key transformations: Deduplicates sites across multiple years of measurements, resolves coordinate conflicts by prioritizing most recent valid coordinates, maintains EPA's standardized site identification scheme for consistent cross-referencing.
+
+   Downstream usage: Joined with core.fct_measurements for geographic analysis, feeds state-level aggregations in mart tables, enables spatial clustering and proximity analysis, supports regulatory compliance reporting by administrative boundaries.
+
+   Data lineage: Raw EPA AQS CSV files → staging.stg_measurements (normalization) → core.dim_site (deduplication + coordinate resolution).
+
+   Business context: Essential for environmental compliance monitoring, public health impact assessments, air quality trend analysis by geographic region, and policy development targeting specific states or counties with poor air quality.
+ tags:
+   - domain:environmental
+   - data_type:dimension_table
+   - source:epa_aqs
+   - pipeline_role:core
+   - granularity:monitoring_site
+   - geographic_scope:us_national
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - quality:validated
+   - update_pattern:replace
+   - regulatory:clean_air_act
+
  materialization:
    type: table

  depends:
    - staging.stg_measurements

  columns:
    - name: site_id
-     type: varchar
-     description: Surrogate site key (state_county_site)
+     type: VARCHAR
+     description: |
+       Composite primary key constructed as 'state_county_site' using EPA standardized location codes.
+       Format: zero-padded 2-digit state code + underscore + 3-digit county code + underscore + 4-digit site number
+       (e.g., '06_037_1103' for a Los Angeles County site). Provides hierarchical geographic grouping
+       enabling rollups to county and state levels while maintaining unique site identification across
+       the national monitoring network. This key remains stable across years even if monitoring equipment is upgraded.
      checks:
        - name: not_null
        - name: unique
-
    - name: state_name
-     type: varchar
-     description: US state name
+     type: VARCHAR
+     description: |
+       Official US state name as recorded in EPA AQS data (e.g., 'California', 'Texas', 'New York').
+       Includes all 50 states plus District of Columbia and US territories with active monitoring sites.
+       Critical dimension for state-level environmental policy analysis, regulatory compliance reporting,
+       and cross-state air quality comparisons. Trimmed of whitespace during staging for data consistency.
      checks:
        - name: not_null
-
    - name: county_name
-     type: varchar
-     description: County name
-
+     type: VARCHAR
+     description: |
+       County or equivalent administrative subdivision name within the state where the monitoring site is located.
+       Essential for local air quality assessments, county-level regulatory compliance reporting under the Clean Air Act,
+       and identifying non-attainment areas. May include parishes (Louisiana), boroughs (Alaska), or special districts
+       depending on state administrative structure. Used extensively in mart tables for sub-state geographic analysis.
    - name: site_num
-     type: varchar
-     description: EPA monitoring site number
-
+     type: VARCHAR
+     description: |
+       EPA monitoring site number as a 4-digit, zero-padded string (e.g., '0023', '1103').
+       Unique within each state-county combination and represents the specific monitoring station location.
+       This identifier remains consistent across years for temporal trend analysis, enabling tracking
+       of long-term air quality changes at specific geographic points. Does not change when monitoring
+       equipment is upgraded or replaced at the same location.
    - name: latitude
-     type: double
-     description: Site latitude
-
+     type: DOUBLE
+     description: |
+       Monitoring site latitude in decimal degrees using WGS84 datum (North American standard).
+       Used for spatial analysis, distance calculations, and geographic visualization of air quality data.
+       Values range approximately from 18°N (southern Florida/Hawaii) to 71°N (northern Alaska).
+       Derived using most recent non-null coordinates per site to handle equipment relocations while
+       preserving spatial analysis capability. May be null for sites with incomplete EPA geographic metadata.
    - name: longitude
-     type: double
-     description: Site longitude
+     type: DOUBLE
+     description: |-
+       Monitoring site longitude in decimal degrees using WGS84 datum. All values are negative representing
+       western hemisphere locations (-67°W to -180°W covering continental US, Alaska, Hawaii, and territories).
+       Critical for mapping applications, spatial clustering analysis, and proximity-based air quality studies.
+       Derived using most recent non-null coordinates per site to ensure data quality for downstream geographic analysis.
+       May be null for sites with incomplete EPA geographic metadata.

  @bruin */

  SELECT DISTINCT
      site_id,
      state_name,
      county_name,
      site_num,
      -- Use the most recent non-null coordinate per site
      FIRST_VALUE(latitude  IGNORE NULLS) OVER (
          PARTITION BY site_id ORDER BY year DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS latitude,
      FIRST_VALUE(longitude IGNORE NULLS) OVER (
          PARTITION BY site_id ORDER BY year DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS longitude

  FROM staging.stg_measurements

  QUALIFY ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY year DESC) = 1

+67 additions, -17 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)

  SELECT DISTINCT
      site_id,
      state_name,
      county_name,
      site_num,
      -- Use the most recent non-null coordinate per site
      FIRST_VALUE(latitude  IGNORE NULLS) OVER (
          PARTITION BY site_id ORDER BY year DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS latitude,
      FIRST_VALUE(longitude IGNORE NULLS) OVER (
          PARTITION BY site_id ORDER BY year DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS longitude

  FROM staging.stg_measurements

  QUALIFY ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY year DESC) = 1

+67 additions, -17 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)

  name: core.fct_measurements
  type: duckdb.sql
+ description: |
+   Central fact table containing aggregated EPA Air Quality System (AQS) measurements for the five major criteria pollutants monitored across the United States from 2019-2023.
+
+   This table represents the core analytical layer that aggregates raw monitoring data to one record per site-pollutant-year combination. When multiple monitoring instruments (POCs) exist at a single site for the same pollutant, the mean_value is averaged across all instruments to provide a unified annual concentration for analytical purposes.
+
+   The data undergoes quality filtering to exclude null, negative, or invalid measurements, ensuring all values represent legitimate air quality readings. The table serves as the primary source for national air quality trend analysis, state comparisons, and pollutant ranking reports in downstream mart tables.
+
+   Key business insights: Provides foundation for understanding long-term air quality trends, regulatory compliance monitoring, and public health impact assessments across different geographic regions and pollutant types.
+ tags:
+   - domain:environmental
+   - data_type:fact_table
+   - source:epa_aqs
+   - granularity:annual
+   - pipeline_role:core
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - quality:high_governance

  materialization:
    type: table

  depends:
    - staging.stg_measurements
    - core.dim_site

  columns:
    - name: measurement_id
      type: VARCHAR
-     description: Surrogate key (site_id + pollutant + year)
+     description: |
+       Surrogate primary key constructed as site_id || '__' || pollutant || '__' || year.
+       Unique identifier ensuring one record per monitoring site-pollutant-year combination.
+       Example format: '01_073_0023__PM2.5__2023'
      checks:
        - name: not_null
        - name: unique
    - name: site_id
      type: VARCHAR
-     description: Foreign key to core.dim_site
+     description: |
+       Foreign key reference to core.dim_site. Composite identifier in format 'state_county_site'
+       using EPA standardized codes (e.g., '06_037_1103' for Los Angeles). Links to geographic
+       and location metadata for spatial analysis.
      checks:
        - name: not_null
    - name: pollutant
      type: VARCHAR
-     description: Normalized pollutant short name
+     description: |
+       Standardized pollutant identifier for the five major criteria air pollutants regulated
+       under the Clean Air Act. Normalized from verbose EPA parameter names to short codes
+       for consistent analysis across different monitoring programs.
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
-     description: Measurement year
+     description: |
+       Calendar year of the measurement (2019-2023 range). Represents the annual reporting
+       period for EPA AQS data. Used for temporal trend analysis and year-over-year comparisons.
      checks:
        - name: not_null
    - name: mean_value
      type: DOUBLE
-     description: Annual arithmetic mean concentration
+     description: |
+       Annual arithmetic mean concentration averaged across all monitoring instruments (POCs)
+       at the site for the given pollutant-year combination. Represents the primary air quality
+       metric used for regulatory compliance and health impact assessments.
+       Units vary by pollutant: µg/m³ for particulates, ppm for gases.
      checks:
        - name: not_null
        - name: non_negative
    - name: unit
      type: VARCHAR
-     description: Unit of measure
+     description: |
+       Unit of measurement for the concentration value. Standardized EPA units:
+       'Micrograms/cubic meter (LC)' for PM2.5, 'Parts per million' for gaseous pollutants.
+       Critical for proper interpretation and comparison of concentration levels across pollutants.
    - name: parameter_name
      type: VARCHAR
+     description: |-
+       Original EPA parameter name from source data (e.g., 'PM2.5 - Local Conditions',
+       'Carbon monoxide'). Preserved for traceability back to EPA AQS official parameter
+       definitions and regulatory reference documentation.

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

+45 additions, -6 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/marts/mart_state_comparison.asset.sql

✓ Enhanced 'mart_state_comparison.asset.sql'

Changes:
  /* @bruin

  name: marts.mart_state_comparison
  type: duckdb.sql
+ description: |
+   State-level air quality analysis table comparing each US state's pollution performance against national averages for EPA criteria pollutants (2019-2023).
+
+   This mart aggregates monitoring site data to the state level, providing key metrics for environmental policy analysis, public health assessments, and regulatory compliance tracking. Each row represents one state-pollutant-year combination with comparative statistics.
+
+   The table enables identification of the most and least polluted states, tracks state-level air quality trends over time, and quantifies how far each state deviates from national pollution averages. State rankings are calculated within each pollutant-year combination to facilitate cross-state comparisons.
+
+   Key use cases: State environmental scorecards, regulatory compliance monitoring, public health impact assessments, policy effectiveness evaluation, and identifying states requiring targeted air quality interventions.
+
+   Data quality notes: States with fewer than one monitoring site for a given pollutant-year may show less reliable averages. Alaska and Hawaii may have limited monitoring coverage compared to continental US states.
+ tags:
+   - domain:environmental
+   - data_type:mart_table
+   - source:epa_aqs
+   - granularity:state_annual
+   - pipeline_role:mart
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - analytics_use:comparative_analysis
+   - governance:regulatory_compliance
+   - geographic_scope:us_states
+
  materialization:
    type: table

  depends:
    - core.fct_measurements
    - core.dim_site

  columns:
    - name: state_name
-     type: varchar
-     description: US state name
+     type: VARCHAR
+     description: |
+       Official US state name (e.g., 'California', 'Texas'). All 50 states plus DC and territories
+       with EPA monitoring sites included. Used for geographic filtering and reporting breakdowns.
      checks:
        - name: not_null
-
    - name: pollutant
-     type: varchar
-     description: Pollutant short name
+     type: VARCHAR
+     description: |
+       EPA criteria pollutant identifier standardized to short codes. One of five major air quality
+       indicators regulated under the Clean Air Act: PM2.5 (fine particulate matter), Ozone (ground-level),
+       CO (carbon monoxide), NO2 (nitrogen dioxide), SO2 (sulfur dioxide). Critical for pollutant-specific analysis.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of measurements (2019-2023 range). Represents the annual EPA AQS reporting period.
+       Used for temporal trend analysis and year-over-year state performance comparisons.
      checks:
        - name: not_null
-
    - name: avg_mean_value
-     type: double
-     description: State average of site-level annual means
-
+     type: DOUBLE
+     description: |
+       State-level arithmetic mean of all monitoring sites' annual average concentrations within the state.
+       Represents the typical air quality across the state for the given pollutant-year. Units vary by
+       pollutant (µg/m³ for PM2.5, ppm for gases). This is the primary metric for state air quality assessment.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: max_mean_value
-     type: double
-     description: Worst site reading in the state
-
+     type: DOUBLE
+     description: |
+       Highest annual average concentration recorded at any monitoring site within the state for the
+       given pollutant-year. Identifies pollution hotspots and worst-case air quality conditions within
+       each state. Critical for understanding intra-state pollution variability.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: site_count
-     type: integer
-     description: Number of monitoring sites in the state
-
+     type: BIGINT
+     description: |
+       Number of EPA monitoring sites reporting valid annual data within the state for the specific
+       pollutant-year combination. Indicates data reliability and geographic coverage density.
+       Higher counts suggest more representative state averages.
+     checks:
+       - name: not_null
+       - name: positive
    - name: national_avg
-     type: double
-     description: National average for the same pollutant and year
-
+     type: DOUBLE
+     description: |
+       National arithmetic mean concentration for the same pollutant-year combination across all US
+       monitoring sites. Baseline metric for comparative analysis enabling identification of states
+       performing better or worse than the national average.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: pct_above_national
-     type: double
-     description: How many % above/below the national average
-
+     type: DOUBLE
+     description: |
+       Percentage deviation of state average from national average ((state_avg - national_avg) / national_avg * 100).
+       Positive values indicate above-national pollution levels, negative values indicate below-national levels.
+       Key metric for identifying environmental leaders and laggards among states.
    - name: state_rank
-     type: integer
-     description: State rank (1 = most polluted) within pollutant+year
+     type: BIGINT
+     description: |
+       State ranking within each pollutant-year combination ordered by avg_mean_value (1 = highest pollution,
+       50+ = lowest pollution). Enables easy identification of most and least polluted states for each
+       pollutant-year. Essential for state-to-state competitive analysis and policy benchmarking.
+     checks:
+       - name: not_null
+       - name: positive

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

+87 additions, -26 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/marts/mart_aqi_annual_trends.asset.sql

✓ Enhanced 'mart_aqi_annual_trends.asset.sql'

Changes:
  /* @bruin

  name: marts.mart_aqi_annual_trends
  type: duckdb.sql
+ description: |
+   National-level annual air quality trends summary for the five major EPA criteria pollutants (PM2.5, Ozone, CO, NO2, SO2) from 2019-2023.
+
+   This mart aggregates site-level measurements to provide a national perspective on air quality patterns, calculating key statistical measures including central tendencies and distribution percentiles. The table enables trend analysis through year-over-year percentage change calculations, making it the primary resource for understanding long-term national air quality improvements or deterioration.
+
+   Each row represents one pollutant-year combination with national statistics computed across all EPA AQS monitoring sites. The year-over-year change metric uses the previous year as baseline, so 2019 data will have NULL yoy_change_pct values. Statistical measures are rounded to 4 decimal places for concentration values and 2 decimal places for percentage changes to balance precision with readability.
+
+   Primary use cases include regulatory compliance reporting, public health trend assessment, environmental policy impact evaluation, and comparative analysis across pollutant types. The table serves data consumers ranging from EPA analysts to environmental researchers and public health officials.
+ tags:
+   - domain:environmental
+   - data_type:mart
+   - source:epa_aqs
+   - granularity:annual
+   - pipeline_role:mart
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - analysis_type:trend_analysis
+   - scope:national
+   - update_pattern:append_only
+
  materialization:
    type: table

  depends:
    - core.fct_measurements

  columns:
    - name: pollutant
-     type: varchar
-     description: Pollutant short name
+     type: VARCHAR
+     description: |
+       Standardized pollutant identifier (PM2.5, Ozone, CO, NO2, SO2). These represent the five major criteria air pollutants
+       regulated under the Clean Air Act. Normalized from verbose EPA parameter names to enable consistent analysis and reporting
+       across different monitoring programs and time periods.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of the aggregated measurements (2019-2023 coverage). Represents the EPA AQS annual reporting period.
+       Used as the temporal dimension for trend analysis and year-over-year comparison calculations.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - 2019
+           - 2020
+           - 2021
+           - 2022
+           - 2023
    - name: avg_mean_value
-     type: double
-     description: National average of site-level annual means
-
+     type: DOUBLE
+     description: |
+       National arithmetic mean of site-level annual concentration averages, rounded to 4 decimal places. Computed by averaging
+       the mean_value from all EPA monitoring sites for the given pollutant-year combination. Units vary by pollutant:
+       µg/m³ for PM2.5, ppm for gaseous pollutants (Ozone, CO, NO2, SO2). This is the primary metric for national air quality
+       trend assessment and regulatory compliance evaluation.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: median_mean_value
-     type: double
-     description: National median of site-level annual means
-
+     type: DOUBLE
+     description: |
+       National median of site-level annual concentration averages, rounded to 4 decimal places. Provides a measure of central
+       tendency less sensitive to outlier monitoring sites than the arithmetic mean. Useful for understanding typical air quality
+       conditions while minimizing the influence of extremely high or low measurement sites that may skew the national average.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: p10_value
-     type: double
-     description: 10th percentile across all sites
-
+     type: DOUBLE
+     description: |
+       10th percentile of site-level annual concentration averages, rounded to 4 decimal places. Represents the concentration
+       level below which 10% of monitoring sites fall, effectively indicating the cleanest air quality conditions across the
+       national monitoring network. Useful for identifying best-case air quality scenarios and setting improvement targets.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: p90_value
-     type: double
-     description: 90th percentile across all sites
-
+     type: DOUBLE
+     description: |
+       90th percentile of site-level annual concentration averages, rounded to 4 decimal places. Represents the concentration
+       level above which only 10% of monitoring sites exceed, effectively indicating areas with the highest pollution burden.
+       Critical for identifying environmental justice concerns and prioritizing regulatory intervention in the most affected regions.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: site_count
-     type: integer
-     description: Number of monitoring sites reporting
-
+     type: BIGINT
+     description: |
+       Total number of distinct EPA AQS monitoring sites contributing data for the pollutant-year combination. Indicates the
+       statistical robustness of the national aggregates - higher site counts generally provide more reliable trend indicators.
+       Changes in site_count over time may reflect expansion or reduction of the monitoring network, which should be considered
+       when interpreting year-over-year trends.
+     checks:
+       - name: not_null
+       - name: positive
    - name: yoy_change_pct
-     type: double
-     description: Year-over-year % change in national average
+     type: DOUBLE
+     description: |-
+       Year-over-year percentage change in the national average concentration (avg_mean_value), rounded to 2 decimal places.
+       Calculated as ((current_year - previous_year) / previous_year) * 100. NULL for the first year of each pollutant (2019)
+       since no prior baseline exists. Negative values indicate air quality improvement (decreasing pollution), while positive
+       values suggest deterioration. This is the primary metric for assessing the effectiveness of air quality regulations and policies.

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

+91 additions, -23 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/marts/mart_pollutant_ranking.asset.sql

✓ Enhanced 'mart_pollutant_ranking.asset.sql'

Changes:
  /* @bruin

  name: marts.mart_pollutant_ranking
  type: duckdb.sql
+ description: |
+   Comprehensive air quality ranking analysis that ranks EPA monitoring sites by pollutant concentration levels both nationally and within each state for the five major criteria pollutants (PM2.5, Ozone, CO, NO2, SO2) from 2019-2023.
+
+   This mart provides critical insights into which geographic areas experience the highest pollution burden by creating dual ranking systems. National rankings identify the worst air quality sites across the entire United States, while state rankings enable within-state comparisons and identification of local pollution hotspots. Higher ranks indicate higher pollutant concentrations and worse air quality conditions.
+
+   The ranking methodology uses dense ranking based on annual mean concentrations, meaning sites with identical concentration values receive the same rank. This approach ensures fair comparison while handling cases where multiple monitoring sites may have exactly the same measured values due to rounding or measurement precision limitations.
+
+   Key business applications include environmental justice analysis (identifying disproportionately affected communities), regulatory enforcement prioritization, public health advisory targeting, and comparative performance assessment between states. The dual ranking structure enables both broad national policy insights and granular local environmental management decisions.
+
+   Data refresh occurs monthly as part of the EPA AQS pipeline, with rankings recalculated across the complete historical dataset to ensure consistency when new annual data becomes available. Each pollutant maintains separate ranking distributions due to different concentration scales and health impact thresholds.
+ tags:
+   - domain:environmental
+   - data_type:mart
+   - source:epa_aqs
+   - granularity:annual
+   - pipeline_role:mart
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - analysis_type:ranking_analysis
+   - scope:national_and_state
+   - update_pattern:snapshot
+   - use_case:pollution_hotspots
+
  materialization:
    type: table

  depends:
    - core.fct_measurements
    - core.dim_site

  columns:
    - name: site_id
-     type: varchar
-     description: Monitoring site identifier
+     type: VARCHAR
+     description: |
+       Foreign key reference to core.dim_site. EPA monitoring site identifier in composite format 'state_county_site'
+       using standardized EPA codes (e.g., '06_037_1103' for Los Angeles). Links to detailed geographic metadata for
+       spatial analysis and location-based filtering. Essential for mapping pollution hotspots and understanding
+       geographic distribution of air quality impacts.
      checks:
        - name: not_null
-
    - name: state_name
-     type: varchar
-     description: US state name
-
+     type: VARCHAR
+     description: |
+       Full US state name derived from EPA geographic references. Used for state-level aggregation and filtering in
+       analysis workflows. Enables state-to-state air quality comparisons and supports state-specific regulatory
+       reporting requirements. Standardized to official state names for consistency across the dataset.
+     checks:
+       - name: not_null
    - name: county_name
-     type: varchar
-     description: County name
-
+     type: VARCHAR
+     description: |
+       County name within the state where the monitoring site is located. Provides sub-state geographic granularity
+       for local air quality analysis. Critical for identifying county-level pollution patterns and supporting
+       local government environmental planning efforts. Some sites may have NULL county designations for special
+       monitoring locations.
    - name: pollutant
-     type: varchar
-     description: Pollutant short name
+     type: VARCHAR
+     description: |
+       Standardized EPA criteria pollutant identifier (PM2.5, Ozone, CO, NO2, SO2). These five pollutants represent
+       the major air quality indicators regulated under the Clean Air Act due to their significant public health impacts.
+       Rankings are calculated independently for each pollutant due to different concentration scales, health thresholds,
+       and regulatory standards.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of the air quality measurement (2019-2023 coverage). Rankings are calculated independently within
+       each year to account for temporal variations in air quality conditions, regulatory changes, and monitoring network
+       adjustments. Used for trend analysis to identify whether high-ranking sites consistently show poor air quality
+       over time.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - 2019
+           - 2020
+           - 2021
+           - 2022
+           - 2023
    - name: mean_value
-     type: double
-     description: Annual mean concentration
-
+     type: DOUBLE
+     description: |
+       Annual arithmetic mean pollutant concentration in standardized units, rounded to 4 decimal places. This is the
+       primary metric used for ranking calculations. Values represent site-level averages across all monitoring
+       instruments and observation periods during the year. Units vary by pollutant: µg/m³ for PM2.5, ppm for gaseous
+       pollutants (Ozone, CO, NO2, SO2). Higher values indicate worse air quality conditions.
+     checks:
+       - name: not_null
+       - name: non_negative
    - name: unit
-     type: varchar
-     description: Unit of measure
-
+     type: VARCHAR
+     description: |
+       Standardized EPA unit of measurement for the concentration value. Critical for proper interpretation of
+       rankings and concentration comparisons. 'Micrograms/cubic meter (LC)' for particulate matter (PM2.5),
+       'Parts per million' for gaseous pollutants. Units remain consistent within each pollutant type but vary
+       across different pollutant categories due to measurement methodology differences.
+     checks:
+       - name: not_null
    - name: national_rank
-     type: integer
-     description: Site rank nationally (1 = highest concentration) within pollutant+year
-
+     type: BIGINT
+     description: |
+       Dense rank of the monitoring site within all US sites for the specific pollutant-year combination, ordered by
+       descending concentration (rank 1 = highest concentration = worst air quality). Enables identification of the
+       most polluted locations nationally and supports federal environmental justice initiatives. Tied concentrations
+       receive identical ranks, with subsequent ranks continuing sequentially without gaps.
+     checks:
+       - name: not_null
+       - name: positive
    - name: state_rank
-     type: integer
-     description: Site rank within state for pollutant+year
+     type: BIGINT
+     description: |
+       Dense rank of the monitoring site within its state for the specific pollutant-year combination, ordered by
+       descending concentration (rank 1 = highest concentration within state = worst state-level air quality).
+       Enables state-level environmental management and supports local air quality improvement targeting.
+       Essential for intrastate comparisons and state regulatory prioritization efforts.
+     checks:
+       - name: not_null
+       - name: positive

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

+103 additions, -26 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/staging/stg_measurements.asset.sql

✓ Enhanced 'stg_measurements.asset.sql'

Changes:
  /* @bruin

  name: staging.stg_measurements
  type: duckdb.sql
+ description: |
+   Staging layer for EPA Air Quality System (AQS) annual concentration measurements, covering 2019-2023 data for five major criteria pollutants regulated under the Clean Air Act.
+
+   This table performs critical data normalization and quality filtering on raw EPA monitoring data:
+   - Standardizes pollutant names from verbose EPA parameter names to short codes (e.g., "PM2.5 - Local Conditions" → "PM2.5")
+   - Constructs composite site_id keys using EPA's hierarchical location coding (state_county_site format)
+   - Filters to exclude null, negative, or invalid concentration measurements
+   - Preserves Parameter Occurrence Code (POC) values for downstream aggregation across multiple monitoring instruments
+
+   The data represents annual arithmetic mean concentrations from EPA's continuous and manual monitoring networks. Multiple monitoring instruments (different POCs) may exist at a single site for the same pollutant, which are later aggregated in the core layer.
+
+   Downstream usage: Feeds both core.dim_site (unique monitoring locations) and core.fct_measurements (aggregated annual measurements) for air quality trend analysis, regulatory compliance reporting, and public health assessments.
+ tags:
+   - domain:environmental
+   - data_type:staging_table
+   - source:epa_aqs
+   - pipeline_role:staging
+   - granularity:site_pollutant_year_poc
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - update_pattern:replace
+   - quality:validated
+
  materialization:
    type: table

  depends:
    - raw.download_epa

  columns:
    - name: site_id
-     type: varchar
-     description: Unique site identifier (state_county_site)
+     type: VARCHAR
+     description: |
+       Composite primary key constructed as 'state_county_site' using EPA standardized location codes.
+       Format: zero-padded 2-digit state + 3-digit county + 4-digit site number (e.g., '06_037_1103').
+       Provides hierarchical geographic grouping and unique identification across the national monitoring network.
      checks:
        - name: not_null
-
    - name: state_name
-     type: varchar
-     description: US state name
-
+     type: VARCHAR
+     description: |
+       US state name as provided in EPA AQS data. Used for geographic analysis and state-level aggregations
+       in downstream mart tables. Trimmed to remove leading/trailing whitespace from source data.
    - name: county_name
-     type: varchar
-     description: County name
-
+     type: VARCHAR
+     description: |
+       County name within the state where the monitoring site is located. Essential for local air quality
+       assessments and county-level regulatory compliance reporting. May include special districts or parishes
+       depending on state administrative structure.
    - name: site_num
-     type: varchar
-     description: EPA site number
-
+     type: VARCHAR
+     description: |
+       EPA monitoring site number (4-digit, zero-padded). Unique within a state-county combination.
+       Represents the specific monitoring station location and remains consistent across years for
+       temporal trend analysis. Does not change if monitoring equipment is upgraded or replaced.
    - name: latitude
-     type: double
-     description: Site latitude
-
+     type: DOUBLE
+     description: |
+       Monitoring site latitude in decimal degrees (WGS84 datum). Used for spatial analysis, mapping,
+       and proximity calculations. May be null for sites with incomplete geographic metadata.
+       Coordinates can shift slightly over time if monitoring equipment is relocated within the same site designation.
    - name: longitude
-     type: double
-     description: Site longitude
-
+     type: DOUBLE
+     description: |
+       Monitoring site longitude in decimal degrees (WGS84 datum). Used for spatial analysis and geographic
+       visualization of air quality data. May be null for sites with incomplete geographic metadata.
+       Negative values represent western hemisphere locations (all US monitoring sites).
    - name: pollutant
-     type: varchar
-     description: Normalized pollutant short name
+     type: VARCHAR
+     description: |
+       Standardized pollutant identifier normalized from EPA's verbose parameter names to short codes.
+       Covers the five major criteria pollutants: PM2.5 (fine particulate matter), Ozone (ground-level ozone),
+       CO (carbon monoxide), NO2 (nitrogen dioxide), SO2 (sulfur dioxide). Used for consistent cross-pollutant
+       analysis and simplified mart table design.
      checks:
        - name: not_null
-
+       - name: accepted_values
+         value:
+           - PM2.5
+           - Ozone
+           - CO
+           - NO2
+           - SO2
    - name: parameter_name
-     type: varchar
-     description: Original EPA parameter name
-
+     type: VARCHAR
+     description: |
+       Original EPA parameter name from source AQS data (e.g., 'PM2.5 - Local Conditions', 'Carbon monoxide').
+       Preserved for traceability back to EPA's official parameter definitions and regulatory documentation.
+       Essential for data lineage auditing and ensuring compliance with EPA reporting standards.
    - name: mean_value
-     type: double
-     description: Annual arithmetic mean concentration
+     type: DOUBLE
+     description: |
+       Annual arithmetic mean concentration for the pollutant at this monitoring site. Represents the primary
+       air quality metric used for regulatory compliance and health impact assessments. Units vary by pollutant
+       (µg/m³ for PM2.5, ppm for gases). Values are filtered to exclude nulls and negatives during staging.
      checks:
        - name: not_null
-
+       - name: non_negative
    - name: unit
-     type: varchar
-     description: Unit of measure
-
+     type: VARCHAR
+     description: |
+       Unit of measurement for the concentration value. Standardized EPA units include 'Micrograms/cubic meter (LC)'
+       for particulate matter (PM2.5) and 'Parts per million' for gaseous pollutants (Ozone, CO, NO2, SO2).
+       Critical for proper interpretation and comparison of concentration levels across different pollutant types.
    - name: year
-     type: integer
-     description: Measurement year
+     type: INTEGER
+     description: |
+       Calendar year of the annual measurement (2019-2023 range). Represents the EPA AQS annual reporting
+       period used for regulatory compliance monitoring. Essential dimension for temporal trend analysis
+       and year-over-year air quality comparisons.
      checks:
        - name: not_null
+   - name: poc
+     type: INTEGER
+     description: |-
+       Parameter Occurrence Code - EPA's identifier for distinguishing between multiple monitoring instruments
+       measuring the same pollutant at a single site. Integer values (1, 2, 3, etc.) represent different
+       monitoring methods, instruments, or sampling frequencies. Multiple POCs are later aggregated in
+       core.fct_measurements to provide unified site-level concentrations.

  @bruin */

  SELECT
      -- Composite site key
      LPAD(CAST("State Code" AS VARCHAR), 2, '0')
          || '_' || LPAD(CAST("County Code" AS VARCHAR), 3, '0')
          || '_' || LPAD(CAST("Site Num" AS VARCHAR), 4, '0')               AS site_id,

      TRIM("State Name")                                                      AS state_name,
      TRIM("County Name")                                                     AS county_name,
      LPAD(CAST("Site Num" AS VARCHAR), 4, '0')                              AS site_num,

      TRY_CAST("Latitude"  AS DOUBLE)                                        AS latitude,
      TRY_CAST("Longitude" AS DOUBLE)                                        AS longitude,

      -- Normalize pollutant names to short labels
      CASE TRIM("Parameter Name")
          WHEN 'PM2.5 - Local Conditions' THEN 'PM2.5'
          WHEN 'Ozone'                    THEN 'Ozone'
          WHEN 'Carbon monoxide'          THEN 'CO'
          WHEN 'Nitrogen dioxide (NO2)'   THEN 'NO2'
          WHEN 'Sulfur dioxide'           THEN 'SO2'
          ELSE TRIM("Parameter Name")
      END                                                                     AS pollutant,

      TRIM("Parameter Name")                                                  AS parameter_name,

      TRY_CAST("Arithmetic Mean" AS DOUBLE)                                  AS mean_value,
      TRIM("Units of Measure")                                               AS unit,
      CAST("Year" AS INTEGER)                                                AS year,
      CAST("POC" AS INTEGER)                                                 AS poc

  FROM read_csv_auto('epa-air-quality/data/epa_combined.csv', header=true, ignore_errors=true)

  WHERE
      "Arithmetic Mean" IS NOT NULL
      AND TRY_CAST("Arithmetic Mean" AS DOUBLE) >= 0
      AND "Year" IS NOT NULL
      AND "Parameter Name" IN (
          'PM2.5 - Local Conditions',
          'Ozone',
          'Carbon monoxide',
          'Nitrogen dioxide (NO2)',
          'Sulfur dioxide'
      )

+93 additions, -32 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
$ bruin ai enhance epa-air-quality/assets/core/dim_site.asset.sql

✓ Enhanced 'dim_site.asset.sql'

Changes:
  /* @bruin

  name: core.dim_site
  type: duckdb.sql
+ description: |
+   EPA Air Quality System (AQS) monitoring site dimension table containing unique geographic and administrative metadata for air quality monitoring stations across the United States (2019-2023 period).
+
+   This dimension table provides one record per unique monitoring site, serving as the geographic foundation for air quality analysis across multiple pollutants and years. Sites represent physical monitoring station locations within EPA's national ambient air monitoring network, each equipped to measure one or more criteria pollutants (PM2.5, Ozone, CO, NO2, SO2).
+
+   The table aggregates site information from time-series measurement data, using the most recent non-null coordinates for each site to handle cases where monitoring equipment may have been relocated within the same site designation. This approach ensures coordinate stability for spatial analysis while preserving the EPA's hierarchical location coding system.
+
+   Key transformations: Deduplicates sites across multiple years of measurements, resolves coordinate conflicts by prioritizing most recent valid coordinates, maintains EPA's standardized site identification scheme for consistent cross-referencing.
+
+   Downstream usage: Joined with core.fct_measurements for geographic analysis, feeds state-level aggregations in mart tables, enables spatial clustering and proximity analysis, supports regulatory compliance reporting by administrative boundaries.
+
+   Data lineage: Raw EPA AQS CSV files → staging.stg_measurements (normalization) → core.dim_site (deduplication + coordinate resolution).
+
+   Business context: Essential for environmental compliance monitoring, public health impact assessments, air quality trend analysis by geographic region, and policy development targeting specific states or counties with poor air quality.
+ tags:
+   - domain:environmental
+   - data_type:dimension_table
+   - source:epa_aqs
+   - pipeline_role:core
+   - granularity:monitoring_site
+   - geographic_scope:us_national
+   - refresh_pattern:monthly
+   - sensitivity:public
+   - quality:validated
+   - update_pattern:replace
+   - regulatory:clean_air_act
+
  materialization:
    type: table

  depends:
    - staging.stg_measurements

  columns:
    - name: site_id
-     type: varchar
-     description: Surrogate site key (state_county_site)
+     type: VARCHAR
+     description: |
+       Composite primary key constructed as 'state_county_site' using EPA standardized location codes.
+       Format: zero-padded 2-digit state code + underscore + 3-digit county code + underscore + 4-digit site number
+       (e.g., '06_037_1103' for a Los Angeles County site). Provides hierarchical geographic grouping
+       enabling rollups to county and state levels while maintaining unique site identification across
+       the national monitoring network. This key remains stable across years even if monitoring equipment is upgraded.
      checks:
        - name: not_null
        - name: unique
-
    - name: state_name
-     type: varchar
-     description: US state name
+     type: VARCHAR
+     description: |
+       Official US state name as recorded in EPA AQS data (e.g., 'California', 'Texas', 'New York').
+       Includes all 50 states plus District of Columbia and US territories with active monitoring sites.
+       Critical dimension for state-level environmental policy analysis, regulatory compliance reporting,
+       and cross-state air quality comparisons. Trimmed of whitespace during staging for data consistency.
      checks:
        - name: not_null
-
    - name: county_name
-     type: varchar
-     description: County name
-
+     type: VARCHAR
+     description: |
+       County or equivalent administrative subdivision name within the state where the monitoring site is located.
+       Essential for local air quality assessments, county-level regulatory compliance reporting under the Clean Air Act,
+       and identifying non-attainment areas. May include parishes (Louisiana), boroughs (Alaska), or special districts
+       depending on state administrative structure. Used extensively in mart tables for sub-state geographic analysis.
    - name: site_num
-     type: varchar
-     description: EPA monitoring site number
-
+     type: VARCHAR
+     description: |
+       EPA monitoring site number as a 4-digit, zero-padded string (e.g., '0023', '1103').
+       Unique within each state-county combination and represents the specific monitoring station location.
+       This identifier remains consistent across years for temporal trend analysis, enabling tracking
+       of long-term air quality changes at specific geographic points. Does not change when monitoring
+       equipment is upgraded or replaced at the same location.
    - name: latitude
-     type: double
-     description: Site latitude
-
+     type: DOUBLE
+     description: |
+       Monitoring site latitude in decimal degrees using WGS84 datum (North American standard).
+       Used for spatial analysis, distance calculations, and geographic visualization of air quality data.
+       Values range approximately from 18°N (southern Florida/Hawaii) to 71°N (northern Alaska).
+       Derived using most recent non-null coordinates per site to handle equipment relocations while
+       preserving spatial analysis capability. May be null for sites with incomplete EPA geographic metadata.
    - name: longitude
-     type: double
-     description: Site longitude
+     type: DOUBLE
+     description: |-
+       Monitoring site longitude in decimal degrees using WGS84 datum. All values are negative representing
+       western hemisphere locations (-67°W to -180°W covering continental US, Alaska, Hawaii, and territories).
+       Critical for mapping applications, spatial clustering analysis, and proximity-based air quality studies.
+       Derived using most recent non-null coordinates per site to ensure data quality for downstream geographic analysis.
+       May be null for sites with incomplete EPA geographic metadata.

  @bruin */

  SELECT DISTINCT
      site_id,
      state_name,
      county_name,
      site_num,
      -- Use the most recent non-null coordinate per site
      FIRST_VALUE(latitude  IGNORE NULLS) OVER (
          PARTITION BY site_id ORDER BY year DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS latitude,
      FIRST_VALUE(longitude IGNORE NULLS) OVER (
          PARTITION BY site_id ORDER BY year DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS longitude

  FROM staging.stg_measurements

  QUALIFY ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY year DESC) = 1

+67 additions, -17 deletions

mehme@mehmetok MINGW64 ~/Desktop/bruin (master)
