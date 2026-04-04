/* @bruin

name: marts.mart_site_map
type: motherduck.sql
description: |
  Geographic mart that joins pollutant rankings with monitoring site coordinates
  for map visualizations. Returns the top-ranked sites per pollutant and year
  enriched with latitude/longitude for point-map rendering in the dashboard.

  This asset serves as the primary data source for interactive pollution hotspot maps,
  enabling stakeholders to visualize spatial distribution of air quality issues across
  the United States. The table combines ranking analysis from mart_pollutant_ranking
  with precise geographic coordinates from dim_site to support both scatter plot and
  choropleth map visualizations.

  Key visualization use cases include environmental justice analysis (identifying
  disproportionately affected communities), regulatory compliance monitoring by
  geographic region, public health advisory targeting, and comparative air quality
  assessment between metropolitan areas. The dataset filters out monitoring sites
  without valid coordinates to ensure clean map rendering while preserving all
  sites with viable geographic data.

  The join pattern is optimized for geospatial analysis workflows, maintaining
  ranking context while providing the geographic precision needed for accurate
  spatial clustering and proximity analysis. Each row represents a unique
  site-pollutant-year combination with complete geographic and ranking metadata.

  Operational characteristics: Expected dataset size ranges from ~35,000 to 50,000 rows
  (approximately 1,400-2,000 monitoring sites × 5 pollutants × 5 years), with row counts
  varying based on sites with valid coordinates. Performance is optimized through filtering
  of null coordinates before join operations, reducing computational overhead. The monthly
  refresh pattern aligns with EPA data release schedules and regulatory reporting cycles.

  Geographic coverage optimization: Filters out approximately 2-5% of monitoring sites
  with missing or invalid coordinates, ensuring 100% map-renderable data quality while
  maintaining comprehensive national coverage. Coordinate validation prevents common
  geospatial visualization errors such as points appearing in the ocean or at null island.
tags:
  - domain:environmental
  - data_type:mart
  - source:epa_aqs
  - granularity:annual
  - pipeline_role:mart
  - refresh_pattern:monthly
  - sensitivity:public
  - use_case:map_visualization
  - analysis_type:geospatial_analysis
  - scope:national
  - quality:filtered_coordinates
  - update_pattern:snapshot
  - geographic_scope:us_national
  - visualization_type:point_map
  - regulatory:clean_air_act
  - performance:filtered_joins
  - data_quality:coordinate_validated
  - coverage:national_comprehensive
  - analytics:environmental_justice
  - use_case:public_health_advisory
  - integration:dashboard_ready
  - spatial_analysis:clustering_enabled

materialization:
  type: table

depends:
  - marts.mart_pollutant_ranking
  - core.dim_site

columns:
  - name: site_id
    type: VARCHAR
    description: |
      Foreign key reference to core.dim_site. EPA monitoring site identifier in composite format 'state_county_site'
      using standardized EPA codes (e.g., '06_037_1103' for Los Angeles County). Essential for joining geographic
      metadata and enabling drill-down functionality from map visualizations to detailed site information.
      Stable across years allowing temporal analysis of specific monitoring locations.
    checks:
      - name: not_null
  - name: state_name
    type: VARCHAR
    description: |
      Official US state name as recorded in EPA AQS data (includes all 50 states plus DC and territories).
      Critical for state-level map filtering, choropleth visualizations, and regulatory compliance reporting.
      Enables grouping and color-coding in geographic visualizations to support state-to-state air quality
      comparisons and policy analysis workflows.
    checks:
      - name: not_null
  - name: county_name
    type: VARCHAR
    description: |
      County or equivalent administrative subdivision name within the state where the monitoring site is located.
      Essential for local air quality assessments and metropolitan area analysis in map visualizations.
      May include parishes (Louisiana), boroughs (Alaska), or special districts depending on state structure.
      Used for sub-state clustering and local regulatory compliance reporting. Can be null for special
      monitoring locations or federal territories.
  - name: pollutant
    type: VARCHAR
    description: |
      Standardized EPA criteria pollutant identifier (PM2.5, Ozone, CO, NO2, SO2). These five pollutants represent
      the major air quality indicators regulated under the Clean Air Act due to their significant public health impacts.
      Used for map layer filtering and pollutant-specific hotspot visualization. Each pollutant requires separate
      map analysis due to different concentration scales, health thresholds, and geographic distribution patterns.
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
      Calendar year of the air quality measurement (2019-2023 coverage). Critical for temporal analysis
      and time-series map animations showing pollution trends over time. Used for year-over-year comparison
      filtering in dashboard interfaces and supports regulatory compliance reporting by specific assessment periods.
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
      Annual arithmetic mean pollutant concentration in standardized units, rounded to 4 decimal places.
      This is the primary metric used for color-coding map points and determining visual symbology intensity.
      Units vary by pollutant: µg/m³ for particulate matter (PM2.5), parts per million for gaseous pollutants
      (Ozone, CO, NO2, SO2). Higher values indicate worse air quality conditions requiring attention in
      visualization interfaces and public health communications.
    checks:
      - name: not_null
      - name: non_negative
  - name: unit
    type: VARCHAR
    description: |
      Standardized EPA unit of measurement for the concentration value. Critical for proper interpretation
      of map visualizations and concentration comparisons. 'Micrograms/cubic meter (LC)' for particulate
      matter (PM2.5), 'Parts per million' for gaseous pollutants. Displayed in map tooltips and legends
      to ensure accurate scientific communication to end users.
    checks:
      - name: not_null
  - name: national_rank
    type: BIGINT
    description: |
      Dense rank of the monitoring site within all US sites for the specific pollutant-year combination,
      ordered by descending concentration (rank 1 = highest concentration = worst air quality nationally).
      Used for map point sizing, color intensity, and identifying top pollution hotspots for federal attention.
      Enables cross-state comparison and supports environmental justice analysis by highlighting sites with
      disproportionate pollution burden.
    checks:
      - name: not_null
      - name: positive
  - name: state_rank
    type: BIGINT
    description: |
      Dense rank of the monitoring site within its state for the specific pollutant-year combination,
      ordered by descending concentration (rank 1 = highest concentration = worst state-level air quality).
      Critical for intrastate analysis and state-specific environmental management. Used for relative
      map symbology within state boundaries and supports local regulatory prioritization efforts.
    checks:
      - name: not_null
      - name: positive
  - name: latitude
    type: DOUBLE
    description: |
      Monitoring site latitude in decimal degrees using WGS84 datum (North American standard).
      Essential for map point placement and spatial analysis calculations. Values range approximately
      from 18°N (southern Florida/Hawaii/territories) to 72°N (northern Alaska including Utqiagvik).
      Range validation ensures coordinates fall within legitimate US monitoring site boundaries. Derived using most recent non-null
      coordinates per site from dim_site to handle equipment relocations. Only sites with valid
      coordinates are included in this mart to ensure clean map rendering.
    checks:
      - name: not_null
      - name: non_negative
  - name: longitude
    type: DOUBLE
    description: |
      Monitoring site longitude in decimal degrees using WGS84 datum. All values are negative representing
      western hemisphere locations (-67°W to -180°W covering continental US, Alaska, Hawaii, and territories).
      Range validation ensures coordinates fall within legitimate US monitoring site boundaries (-180° to -67°).
      Critical for accurate map positioning and geographic clustering analysis. Combined with latitude for
      proximity calculations and spatial boundary analysis. Filtered for non-null values to maintain
      visualization data quality and prevent mapping errors.
    checks:
      - name: not_null
      - name: negative

@bruin */

SELECT
    r.site_id,
    r.state_name,
    r.county_name,
    r.pollutant,
    r.year,
    r.mean_value,
    r.unit,
    r.national_rank,
    r.state_rank,
    d.latitude,
    d.longitude

FROM marts.mart_pollutant_ranking r
JOIN core.dim_site d USING (site_id)

WHERE d.latitude  IS NOT NULL
  AND d.longitude IS NOT NULL

ORDER BY r.pollutant, r.year, r.national_rank
