/* @bruin

name: marts.mart_site_timeseries
type: motherduck.sql
description: |
  Site-level annual time series for all EPA monitoring sites, enabling trend analysis
  and year-over-year comparisons at the individual site level (2019–2023).

  Each row is one site+pollutant+year. Includes the site's own YoY change and its
  deviation from the national average that year — used for sparklines on the rankings
  page and site-level deep-dives.
tags:
  - domain:environmental
  - data_type:mart
  - source:epa_aqs
  - granularity:site_pollutant_year
  - pipeline_role:mart
  - refresh_pattern:monthly
  - sensitivity:public

materialization:
  type: table

depends:
  - core.fct_measurements
  - core.dim_site

columns:
  - name: site_id
    type: varchar
    description: EPA monitoring site identifier (state_county_site)
    checks:
      - name: not_null

  - name: state_name
    type: varchar
    description: US state name
    checks:
      - name: not_null

  - name: county_name
    type: varchar
    description: County name

  - name: pollutant
    type: varchar
    description: Normalized pollutant short name
    checks:
      - name: not_null
      - name: accepted_values
        value: ["PM2.5", "Ozone", "CO", "NO2", "SO2"]

  - name: year
    type: integer
    description: Measurement year
    checks:
      - name: not_null

  - name: mean_value
    type: double
    description: Annual mean concentration at this site
    checks:
      - name: not_null
      - name: non_negative

  - name: unit
    type: varchar
    description: Unit of measure

  - name: yoy_change_pct
    type: double
    description: Year-over-year % change at this site (NULL for 2019)

  - name: national_avg
    type: double
    description: National average for the same pollutant+year

  - name: pct_vs_national
    type: double
    description: How far this site is above/below national avg (%)

@bruin */

WITH national AS (
    SELECT
        pollutant,
        year,
        AVG(mean_value) AS national_avg
    FROM core.fct_measurements
    GROUP BY pollutant, year
)

SELECT
    f.site_id,
    d.state_name,
    d.county_name,
    f.pollutant,
    f.year,
    ROUND(f.mean_value, 4)                                                              AS mean_value,
    f.unit,
    ROUND(
        100.0 * (f.mean_value - LAG(f.mean_value) OVER (
            PARTITION BY f.site_id, f.pollutant ORDER BY f.year
        )) / NULLIF(LAG(f.mean_value) OVER (
            PARTITION BY f.site_id, f.pollutant ORDER BY f.year
        ), 0),
        2
    )                                                                                   AS yoy_change_pct,
    ROUND(n.national_avg, 4)                                                            AS national_avg,
    ROUND(
        100.0 * (f.mean_value - n.national_avg) / NULLIF(n.national_avg, 0),
        2
    )                                                                                   AS pct_vs_national

FROM core.fct_measurements f
JOIN core.dim_site         d USING (site_id)
JOIN national              n USING (pollutant, year)

ORDER BY f.site_id, f.pollutant, f.year
