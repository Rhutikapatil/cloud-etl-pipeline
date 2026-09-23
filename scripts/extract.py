"""
Extract stage.

Pulls the public NYT COVID-19 US states time series (real public data,
updated through the dataset's own retirement date) and lands it as a raw,
unmodified CSV under a path structured like an S3 landing-zone key
(`data/lake/raw/<dataset>/<run_date>/...`). In a real AWS deployment this
task would write to `s3://<landing-bucket>/raw/...` instead of local disk —
see infra/main.tf and docs/aws_deployment.md — but the extraction and
validation logic is identical either way, which is the point of keeping it
in a plain function instead of Airflow-specific code.
"""

import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

SOURCE_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv"


def extract_us_states(run_date: str, lake_root: str = "data/lake/raw") -> str:
    """Downloads the source CSV and lands it, unmodified, at a
    landing-zone path partitioned by run date. Returns the landed path."""
    dest_dir = Path(lake_root) / "covid_us_states" / run_date
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "us-states.csv"

    logger.info("Extracting %s -> %s", SOURCE_URL, dest_path)
    resp = requests.get(SOURCE_URL, timeout=60)
    resp.raise_for_status()

    dest_path.write_bytes(resp.content)
    logger.info("Landed %d bytes", len(resp.content))
    return str(dest_path)
