/* @bruin

name: core.dim_site
type: motherduck.sql
description: |
  EPA Air Quality System (AQS) monitoring site dimension table containing unique geographic and administrative metadata for air quality monitoring stations across the United States (2019-2023 period).

  This dimension table provides one record per unique monitoring site, serving as the geographic foundation for air quality analysis across multiple pollutants and years. Sites represent physical monitoring station locations within EPA's national ambient air monitoring network, each equipped to measure one or more criteria pollutants (PM2.5, Ozone, CO, NO2, SO2).

  The table aggregates site information from time-series measurement data, using the most recent non-null coordinates for each site to handle cases where monitoring equipment may have been relocated within the same site designation. This approach ensures coordinate stability for spatial analysis while preserving the EPA's hierarchical location coding system.

  Key transformations: Deduplicates sites across multiple years of measurements, resolves coordinate conflicts by prioritizing most recent valid coordinates, maintains EPA's standardized site identification scheme for consistent cross-referencing.

  Downstream usage: Joined with core.fct_measurements for geographic analysis, feeds state-level aggregations in mart tables, enables spatial clustering and proximity analysis, supports regulatory compliance reporting by administrative boundaries.

  Data lineage: Raw EPA AQS CSV files → staging.stg_measurements (normalization) → core.dim_site (deduplication + coordinate resolution).

  Business context: Essential for environmental compliance monitoring, public health impact assessments, air quality trend analysis by geographic region, and policy development targeting specific states or counties with poor air quality.
tags:
  - domain:environmental
  - data_type:dimension_table
  - source:epa_aqs
  - pipeline_role:core
  - granularity:monitoring_site
  - geographic_scope:us_national
  - refresh_pattern:monthly
  - sensitivity:public
  - quality:validated
  - update_pattern:replace
  - regulatory:clean_air_act

materialization:
  type: table

depends:
  - staging.stg_measurements

columns:
  - name: site_id
    type: VARCHAR
    description: |
      Composite primary key constructed as 'state_county_site' using EPA standardized location codes.
      Format: zero-padded 2-digit state code + underscore + 3-digit county code + underscore + 4-digit site number
      (e.g., '06_037_1103' for a Los Angeles County site). Provides hierarchical geographic grouping
      enabling rollups to county and state levels while maintaining unique site identification across
      the national monitoring network. This key remains stable across years even if monitoring equipment is upgraded.
    checks:
      - name: not_null
      - name: unique
  - name: state_name
    type: VARCHAR
    description: |
      Official US state name as recorded in EPA AQS data (e.g., 'California', 'Texas', 'New York').
      Includes all 50 states plus District of Columbia and US territories with active monitoring sites.
      Critical dimension for state-level environmental policy analysis, regulatory compliance reporting,
      and cross-state air quality comparisons. Trimmed of whitespace during staging for data consistency.
    checks:
      - name: not_null
  - name: county_name
    type: VARCHAR
    description: |
      County or equivalent administrative subdivision name within the state where the monitoring site is located.
      Essential for local air quality assessments, county-level regulatory compliance reporting under the Clean Air Act,
      and identifying non-attainment areas. May include parishes (Louisiana), boroughs (Alaska), or special districts
      depending on state administrative structure. Used extensively in mart tables for sub-state geographic analysis.
  - name: site_num
    type: VARCHAR
    description: |
      EPA monitoring site number as a 4-digit, zero-padded string (e.g., '0023', '1103').
      Unique within each state-county combination and represents the specific monitoring station location.
      This identifier remains consistent across years for temporal trend analysis, enabling tracking
      of long-term air quality changes at specific geographic points. Does not change when monitoring
      equipment is upgraded or replaced at the same location.
  - name: latitude
    type: DOUBLE
    description: |
      Monitoring site latitude in decimal degrees using WGS84 datum (North American standard).
      Used for spatial analysis, distance calculations, and geographic visualization of air quality data.
      Values range approximately from 18°N (southern Florida/Hawaii) to 71°N (northern Alaska).
      Derived using most recent non-null coordinates per site to handle equipment relocations while
      preserving spatial analysis capability. May be null for sites with incomplete EPA geographic metadata.
  - name: longitude
    type: DOUBLE
    description: |-
      Monitoring site longitude in decimal degrees using WGS84 datum. All values are negative representing
      western hemisphere locations (-67°W to -180°W covering continental US, Alaska, Hawaii, and territories).
      Critical for mapping applications, spatial clustering analysis, and proximity-based air quality studies.
      Derived using most recent non-null coordinates per site to ensure data quality for downstream geographic analysis.
      May be null for sites with incomplete EPA geographic metadata.

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
