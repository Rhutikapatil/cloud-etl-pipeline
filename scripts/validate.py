"""
Validation stage — a data-quality gate between raw landing and transform.

Raises DataValidationError on failure, which fails the Airflow task (and,
with the DAG's retry/alerting config, would page a data engineer) rather
than silently passing bad data downstream.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = {"date", "state", "fips", "cases", "deaths"}
EXPECTED_STATE_COUNT_MIN = 50  # 50 states + DC/territories in the real source


class DataValidationError(Exception):
    pass


def validate_us_states(landed_path: str) -> dict:
    df = pd.read_csv(landed_path)

    errors = []

    missing_cols = EXPECTED_COLUMNS - set(df.columns)
    if missing_cols:
        errors.append(f"missing expected columns: {missing_cols}")

    if len(df) == 0:
        errors.append("landed file has zero rows")

    if df["state"].nunique() < EXPECTED_STATE_COUNT_MIN:
        errors.append(f"only {df['state'].nunique()} distinct states found, expected >= {EXPECTED_STATE_COUNT_MIN}")

    null_counts = df[["date", "state", "cases", "deaths"]].isna().sum()
    if null_counts.sum() > 0:
        errors.append(f"unexpected nulls in key columns: {null_counts[null_counts > 0].to_dict()}")

    if (df["cases"] < 0).any() or (df["deaths"] < 0).any():
        errors.append("negative cumulative case/death counts found (should never happen at source)")

    result = {
        "row_count": len(df),
        "state_count": int(df["state"].nunique()),
        "date_min": str(df["date"].min()),
        "date_max": str(df["date"].max()),
        "errors": errors,
    }

    if errors:
        logger.error("Validation failed: %s", errors)
        raise DataValidationError(f"{len(errors)} validation error(s): {errors}")

    logger.info("Validation passed: %s", result)
    return result
