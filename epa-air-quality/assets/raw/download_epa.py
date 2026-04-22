"""@bruin
name: raw.download_epa
type: python

@bruin"""

import io
import os
import time
import zipfile

import duckdb
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


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

session = make_session()
frames = []

for year in YEARS:
    url = BASE_URL.format(year=year)
    dest_csv = os.path.join(OUTPUT_DIR, f"annual_conc_by_monitor_{year}.csv")

    if os.path.exists(dest_csv):
        print(f"[{year}] Already downloaded, skipping.")
        df = pd.read_csv(dest_csv, low_memory=False)
    else:
        print(f"[{year}] Downloading {url} ...")
        for attempt in range(1, 4):
            try:
                resp = session.get(url, timeout=180)
                resp.raise_for_status()
                break
            except Exception as e:
                print(f"[{year}] Attempt {attempt} failed: {e}")
                if attempt == 3:
                    raise
                time.sleep(10 * attempt)

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, low_memory=False)

        df.to_csv(dest_csv, index=False)
        print(f"[{year}] Saved {len(df):,} rows to {dest_csv}")

    df = df[df["Parameter Name"].isin(TARGET_POLLUTANTS)]
    existing_cols = [c for c in COLUMNS_KEEP if c in df.columns]
    df = df[existing_cols]
    frames.append(df)

combined = pd.concat(frames, ignore_index=True)
print(f"Combined dataset: {len(combined):,} rows")

# Write directly to MotherDuck
print("BRUIN_VARS:", os.environ.get("BRUIN_VARS"))
print("BRUIN_VARS_SCHEMA:", os.environ.get("BRUIN_VARS_SCHEMA"))
print("BRUIN_VAULT_PATH:", os.environ.get("BRUIN_VAULT_PATH"))

token = os.environ.get("MOTHERDUCK_TOKEN")
if not token:
    # Fall back to reading from .bruin.yml (local dev)
    import yaml
    bruin_yml = os.path.join(os.path.dirname(__file__), "..", "..", "..", ".bruin.yml")
    if os.path.exists(bruin_yml):
        with open(bruin_yml) as f:
            config = yaml.safe_load(f)
        conns = config.get("environments", {}).get("default", {}).get("connections", {}).get("motherduck", [])
        if conns:
            configured_token = conns[0].get("token", "")
            expanded_token = os.path.expandvars(configured_token)
            if expanded_token and expanded_token != configured_token:
                token = expanded_token
if not token:
    raise KeyError("MotherDuck token not found. Set MOTHERDUCK_TOKEN env var or configure .bruin.yml")
con = duckdb.connect(f"md:epa-air-quality?motherduck_token={token}")
con.execute("CREATE SCHEMA IF NOT EXISTS raw")
con.execute("DROP TABLE IF EXISTS raw.epa_combined")
con.execute("CREATE TABLE raw.epa_combined AS SELECT * FROM combined")
print(f"Written {len(combined):,} rows to MotherDuck raw.epa_combined")
con.close()
