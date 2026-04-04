/* @bruin

name: marts.mart_best_worst_states
type: duckdb.sql
description: |
  Pre-aggregated best/worst state labels per pollutant for KPI cards and overview tables.
  Each row is a state+pollutant with 2019 and 2023 averages, 5-year % change, and a
  category label: most_improved, most_worsened, cleanest_2023, most_polluted_2023.

  Designed for Evidence.dev KPI cards and summary DataTables on the overview page.
tags:
  - domain:environmental
  - data_type:mart
  - source:epa_aqs
  - granularity:state_pollutant
  - pipeline_role:mart
  - refresh_pattern:monthly
  - sensitivity:public

materialization:
  type: table

depends:
  - core.fct_measurements
  - core.dim_site

columns:
  - name: pollutant
    type: varchar
    description: Normalized pollutant short name
    checks:
      - name: not_null

  - name: state_name
    type: varchar
    description: US state name
    checks:
      - name: not_null

  - name: avg_2019
    type: double
    description: State average concentration in 2019

  - name: avg_2023
    type: double
    description: State average concentration in 2023

  - name: pct_change_5yr
    type: double
    description: 5-year % change from 2019 to 2023 (negative = improved)

  - name: site_count_2023
    type: integer
    description: Number of monitoring sites in 2023

  - name: category
    type: varchar
    description: |
      One of: most_improved, most_worsened, cleanest_2023, most_polluted_2023
    checks:
      - name: not_null
      - name: accepted_values
        value: ["most_improved", "most_worsened", "cleanest_2023", "most_polluted_2023"]

@bruin */

WITH state_year AS (
    SELECT
        d.state_name,
        f.pollutant,
        f.year,
        AVG(f.mean_value)       AS avg_value,
        COUNT(DISTINCT f.site_id) AS site_count
    FROM core.fct_measurements f
    JOIN core.dim_site d USING (site_id)
    -- Exclude cross-border/non-US entries
    WHERE d.state_name NOT IN ('Country Of Mexico', 'Virgin Islands', 'Unknown')
    GROUP BY d.state_name, f.pollutant, f.year
),

endpoints AS (
    SELECT
        state_name,
        pollutant,
        MAX(CASE WHEN year = 2019 THEN avg_value END)    AS avg_2019,
        MAX(CASE WHEN year = 2023 THEN avg_value END)    AS avg_2023,
        MAX(CASE WHEN year = 2023 THEN site_count END)   AS site_count_2023
    FROM state_year
    GROUP BY state_name, pollutant
    -- Require data in both endpoints
    HAVING MAX(CASE WHEN year = 2019 THEN avg_value END) IS NOT NULL
       AND MAX(CASE WHEN year = 2023 THEN avg_value END) IS NOT NULL
),

with_change AS (
    SELECT
        *,
        ROUND(100.0 * (avg_2023 - avg_2019) / NULLIF(avg_2019, 0), 2) AS pct_change_5yr
    FROM endpoints
),

ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY pollutant ORDER BY pct_change_5yr ASC)  AS rank_improved,
        RANK() OVER (PARTITION BY pollutant ORDER BY pct_change_5yr DESC) AS rank_worsened,
        RANK() OVER (PARTITION BY pollutant ORDER BY avg_2023 ASC)        AS rank_cleanest,
        RANK() OVER (PARTITION BY pollutant ORDER BY avg_2023 DESC)       AS rank_most_polluted
    FROM with_change
)

-- Most improved (top 10 per pollutant)
SELECT
    pollutant,
    state_name,
    ROUND(avg_2019, 4)   AS avg_2019,
    ROUND(avg_2023, 4)   AS avg_2023,
    pct_change_5yr,
    site_count_2023,
    'most_improved'      AS category
FROM ranked
WHERE rank_improved <= 10

UNION ALL

-- Most worsened (top 10 per pollutant)
SELECT
    pollutant,
    state_name,
    ROUND(avg_2019, 4),
    ROUND(avg_2023, 4),
    pct_change_5yr,
    site_count_2023,
    'most_worsened'
FROM ranked
WHERE rank_worsened <= 10

UNION ALL

-- Cleanest in 2023 (top 10 per pollutant)
SELECT
    pollutant,
    state_name,
    ROUND(avg_2019, 4),
    ROUND(avg_2023, 4),
    pct_change_5yr,
    site_count_2023,
    'cleanest_2023'
FROM ranked
WHERE rank_cleanest <= 10

UNION ALL

-- Most polluted in 2023 (top 10 per pollutant)
SELECT
    pollutant,
    state_name,
    ROUND(avg_2019, 4),
    ROUND(avg_2023, 4),
    pct_change_5yr,
    site_count_2023,
    'most_polluted_2023'
FROM ranked
WHERE rank_most_polluted <= 10

ORDER BY pollutant, category, pct_change_5yr
