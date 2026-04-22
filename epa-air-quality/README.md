# US EPA Air Quality Pipeline

End-to-end Bruin pipeline ingesting EPA AQS annual concentration data (2019–2023)
into a local DuckDB warehouse, with 4 modeling layers.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                     │
│   EPA AQS Bulk CSVs  (annual_conc_by_monitor_{YEAR}.zip, 2019-2023)    │
│   URL: https://aqs.epa.gov/aqsweb/airdata/                             │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │  HTTP download + unzip
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  RAW LAYER  (Python asset)                                              │
│  raw.download_epa                                                       │
│  • Downloads ZIP files for each year                                    │
│  • Filters to: PM2.5, Ozone, CO, NO2, SO2                             │
│  • Writes data/epa_combined.csv                                         │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │  read_csv_auto()
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STAGING LAYER  (DuckDB SQL)                                            │
│  staging.stg_measurements                                               │
│  • Normalizes pollutant names  (e.g. "Carbon monoxide" → "CO")         │
│  • Builds site_id composite key (state_county_site)                    │
│  • Casts types, drops nulls & negatives                                │
└──────────────┬───────────────────────────────────────┬─────────────────┘
               │                                       │
               ▼                                       ▼
┌──────────────────────────┐           ┌───────────────────────────────┐
│  CORE LAYER              │           │  CORE LAYER                   │
│  core.dim_site           │           │  core.fct_measurements        │
│  • One row per site      │           │  • One row per site+pollutant │
│  • State, county, coords │           │    +year                      │
│                          │           │  • Quality checks:            │
│                          │           │    - not_null (4 cols)        │
│                          │           │    - non_negative (mean_value)│
│                          │           │    - accepted_values (pollut.)│
└──────────────────────────┘           └───────────────────┬───────────┘
                                                           │
               ┌───────────────────────────────────────────┤
               │                   │                       │
               ▼                   ▼                       ▼
┌──────────────────┐  ┌───────────────────────┐  ┌────────────────────┐
│  MARTS LAYER     │  │  MARTS LAYER          │  │  MARTS LAYER       │
│  mart_aqi_       │  │  mart_state_          │  │  mart_pollutant_   │
│  annual_trends   │  │  comparison           │  │  ranking           │
│                  │  │                       │  │                    │
│  National trends │  │  State vs. national   │  │  Site rankings     │
│  per pollutant   │  │  avg, state rank      │  │  national + state  │
│  YoY % change    │  │  pct above/below avg  │  │  per pollutant+yr  │
└──────────────────┘  └───────────────────────┘  └────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  MARTS LAYER                                                           │
│  mart_pm25_geo_summary                                                 │
│  • Dashboard-ready PM2.5 state and county summaries                    │
│  • Geography-year averages, site counts, and rankings                  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
epa-air-quality/
├── .bruin.yml               ← (root-level, shared with other pipelines)
├── pipeline.yml             ← Pipeline definition, @monthly schedule
├── requirements.txt         ← Python deps: pandas, requests
├── data/                    ← Downloaded CSVs land here (gitignored)
│   ├── annual_conc_by_monitor_2019.csv
│   ├── ...
│   └── epa_combined.csv     ← Combined filtered dataset
└── assets/
    ├── raw/
    │   └── download_epa.py
    ├── staging/
    │   └── stg_measurements.asset.sql
    ├── core/
    │   ├── dim_site.asset.sql
    │   └── fct_measurements.asset.sql
    └── marts/
        ├── mart_aqi_annual_trends.asset.sql
        ├── mart_pm25_geo_summary.asset.sql
        ├── mart_pollutant_ranking.asset.sql
        └── mart_state_comparison.asset.sql
```

## Pollutants Covered

| Short Name | EPA Parameter Name             |
|------------|-------------------------------|
| PM2.5      | PM2.5 - Local Conditions      |
| Ozone      | Ozone                         |
| CO         | Carbon monoxide               |
| NO2        | Nitrogen dioxide (NO2)        |
| SO2        | Sulfur dioxide                |

## Quality Checks (fct_measurements)

| Column      | Checks                                        |
|-------------|-----------------------------------------------|
| site_id     | not_null                                      |
| pollutant   | not_null, accepted_values (PM2.5/Ozone/CO/NO2/SO2) |
| year        | not_null                                      |
| mean_value  | not_null, non_negative                        |

## Running the Pipeline

```bash
# Validate all assets
bruin validate epa-air-quality/

# Run full pipeline
bruin run epa-air-quality/

# Run a single asset
bruin run epa-air-quality/assets/raw/download_epa.py

# Run from staging onward (skip re-download)
bruin run epa-air-quality/ --downstream staging.stg_measurements
```

## DuckDB Connection

Configured in the root `.bruin.yml`:
```yaml
connections:
  duckdb:
    - name: duckdb-default
      path: duckdb.db
```

The warehouse file lives at `./warehouse.db` relative to where you run Bruin.
