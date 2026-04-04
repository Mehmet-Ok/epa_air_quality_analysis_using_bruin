"""@bruin
name: raw.download_epa
type: python

@bruin"""

import io
import os
import zipfile

import pandas as pd
import requests

YEARS = [2019, 2020, 2021, 2022, 2023]
OUTPUT_DIR = "epa-air-quality/data"
BASE_URL = "https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_{year}.zip"

TARGET_POLLUTANTS = {"PM2.5 - Local Conditions", "Ozone", "Carbon monoxide", "Nitrogen dioxide (NO2)", "Sulfur dioxide"}

COLUMNS_KEEP = [
    "State Name",
    "County Name",
    "Site Num",
    "Parameter Name",
    "Arithmetic Mean",
    "Units of Measure",
    "Year",
    "Latitude",
    "Longitude",
    "Datum",
    "Parameter Code",
    "POC",
    "State Code",
    "County Code",
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

frames = []

for year in YEARS:
    url = BASE_URL.format(year=year)
    dest_csv = os.path.join(OUTPUT_DIR, f"annual_conc_by_monitor_{year}.csv")

    if os.path.exists(dest_csv):
        print(f"[{year}] Already downloaded, skipping.")
        df = pd.read_csv(dest_csv, low_memory=False)
    else:
        print(f"[{year}] Downloading {url} ...")
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, low_memory=False)

        df.to_csv(dest_csv, index=False)
        print(f"[{year}] Saved {len(df):,} rows to {dest_csv}")

    # Filter to target pollutants only
    df = df[df["Parameter Name"].isin(TARGET_POLLUTANTS)]

    # Keep only needed columns (intersect with what actually exists)
    existing_cols = [c for c in COLUMNS_KEEP if c in df.columns]
    df = df[existing_cols]

    frames.append(df)

combined = pd.concat(frames, ignore_index=True)
combined_path = os.path.join(OUTPUT_DIR, "epa_combined.csv")
combined.to_csv(combined_path, index=False)
print(f"Combined dataset: {len(combined):,} rows -> {combined_path}")
