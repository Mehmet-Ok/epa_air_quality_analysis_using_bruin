/* @bruin

name: staging.stg_measurements
type: duckdb.sql
connection: motherduck-default
description: |
  Staging layer for EPA Air Quality System (AQS) annual concentration measurements, covering 2019-2023 data for five major criteria pollutants regulated under the Clean Air Act.

  This table performs critical data normalization and quality filtering on raw EPA monitoring data:
  - Standardizes pollutant names from verbose EPA parameter names to short codes (e.g., "PM2.5 - Local Conditions" → "PM2.5")
  - Constructs composite site_id keys using EPA's hierarchical location coding (state_county_site format)
  - Filters to exclude null, negative, or invalid concentration measurements
  - Preserves Parameter Occurrence Code (POC) values for downstream aggregation across multiple monitoring instruments

  The data represents annual arithmetic mean concentrations from EPA's continuous and manual monitoring networks. Multiple monitoring instruments (different POCs) may exist at a single site for the same pollutant, which are later aggregated in the core layer.

  Downstream usage: Feeds both core.dim_site (unique monitoring locations) and core.fct_measurements (aggregated annual measurements) for air quality trend analysis, regulatory compliance reporting, and public health assessments.
tags:
  - domain:environmental
  - data_type:staging_table
  - source:epa_aqs
  - pipeline_role:staging
  - granularity:site_pollutant_year_poc
  - refresh_pattern:monthly
  - sensitivity:public
  - update_pattern:replace
  - quality:validated

materialization:
  type: table

depends:
  - raw.download_epa

columns:
  - name: site_id
    type: VARCHAR
    description: |
      Composite primary key constructed as 'state_county_site' using EPA standardized location codes.
      Format: zero-padded 2-digit state + 3-digit county + 4-digit site number (e.g., '06_037_1103').
      Provides hierarchical geographic grouping and unique identification across the national monitoring network.
    checks:
      - name: not_null
  - name: state_name
    type: VARCHAR
    description: |
      US state name as provided in EPA AQS data. Used for geographic analysis and state-level aggregations
      in downstream mart tables. Trimmed to remove leading/trailing whitespace from source data.
  - name: county_name
    type: VARCHAR
    description: |
      County name within the state where the monitoring site is located. Essential for local air quality
      assessments and county-level regulatory compliance reporting. May include special districts or parishes
      depending on state administrative structure.
  - name: site_num
    type: VARCHAR
    description: |
      EPA monitoring site number (4-digit, zero-padded). Unique within a state-county combination.
      Represents the specific monitoring station location and remains consistent across years for
      temporal trend analysis. Does not change if monitoring equipment is upgraded or replaced.
  - name: latitude
    type: DOUBLE
    description: |
      Monitoring site latitude in decimal degrees (WGS84 datum). Used for spatial analysis, mapping,
      and proximity calculations. May be null for sites with incomplete geographic metadata.
      Coordinates can shift slightly over time if monitoring equipment is relocated within the same site designation.
  - name: longitude
    type: DOUBLE
    description: |
      Monitoring site longitude in decimal degrees (WGS84 datum). Used for spatial analysis and geographic
      visualization of air quality data. May be null for sites with incomplete geographic metadata.
      Negative values represent western hemisphere locations (all US monitoring sites).
  - name: pollutant
    type: VARCHAR
    description: |
      Standardized pollutant identifier normalized from EPA's verbose parameter names to short codes.
      Covers the five major criteria pollutants: PM2.5 (fine particulate matter), Ozone (ground-level ozone),
      CO (carbon monoxide), NO2 (nitrogen dioxide), SO2 (sulfur dioxide). Used for consistent cross-pollutant
      analysis and simplified mart table design.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - PM2.5
          - Ozone
          - CO
          - NO2
          - SO2
  - name: parameter_name
    type: VARCHAR
    description: |
      Original EPA parameter name from source AQS data (e.g., 'PM2.5 - Local Conditions', 'Carbon monoxide').
      Preserved for traceability back to EPA's official parameter definitions and regulatory documentation.
      Essential for data lineage auditing and ensuring compliance with EPA reporting standards.
  - name: mean_value
    type: DOUBLE
    description: |
      Annual arithmetic mean concentration for the pollutant at this monitoring site. Represents the primary
      air quality metric used for regulatory compliance and health impact assessments. Units vary by pollutant
      (µg/m³ for PM2.5, ppm for gases). Values are filtered to exclude nulls and negatives during staging.
    checks:
      - name: not_null
      - name: non_negative
  - name: unit
    type: VARCHAR
    description: |
      Unit of measurement for the concentration value. Standardized EPA units include 'Micrograms/cubic meter (LC)'
      for particulate matter (PM2.5) and 'Parts per million' for gaseous pollutants (Ozone, CO, NO2, SO2).
      Critical for proper interpretation and comparison of concentration levels across different pollutant types.
  - name: year
    type: INTEGER
    description: |
      Calendar year of the annual measurement (2019-2023 range). Represents the EPA AQS annual reporting
      period used for regulatory compliance monitoring. Essential dimension for temporal trend analysis
      and year-over-year air quality comparisons.
    checks:
      - name: not_null
  - name: poc
    type: INTEGER
    description: |-
      Parameter Occurrence Code - EPA's identifier for distinguishing between multiple monitoring instruments
      measuring the same pollutant at a single site. Integer values (1, 2, 3, etc.) represent different
      monitoring methods, instruments, or sampling frequencies. Multiple POCs are later aggregated in
      core.fct_measurements to provide unified site-level concentrations.

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

FROM raw.epa_combined

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
