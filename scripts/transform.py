"""
Transform stage.

The source is *cumulative* case/death counts per state per day. The
warehouse table this pipeline serves needs *incident* (new) counts per
state per epi week, which requires:

1. Diffing consecutive days within each state to get daily new cases/deaths.
2. Flagging negative diffs rather than silently dropping or clipping them —
   negative diffs are real and expected in public health surveillance data
   (a state's health department revises an earlier cumulative total down,
   e.g. after removing duplicate or misattributed cases), and hiding them
   would misrepresent what happened. They're flagged in a `had_correction`
   column and clipped to zero only for the purposes of a monotonic
   "new cases" figure — the raw diff is preserved in `raw_daily_diff` for
   anyone who needs to audit it.
3. Aggregating daily to epi-week (ISO week) per state.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform_to_weekly(landed_path: str) -> pd.DataFrame:
    df = pd.read_csv(landed_path, parse_dates=["date"])
    df = df.sort_values(["state", "date"])

    df["prev_cases"] = df.groupby("state")["cases"].shift(1)
    df["prev_deaths"] = df.groupby("state")["deaths"].shift(1)
    df["raw_daily_new_cases"] = (df["cases"] - df["prev_cases"]).fillna(df["cases"])
    df["raw_daily_new_deaths"] = (df["deaths"] - df["prev_deaths"]).fillna(df["deaths"])

    df["had_correction"] = (df["raw_daily_new_cases"] < 0) | (df["raw_daily_new_deaths"] < 0)
    df["daily_new_cases"] = df["raw_daily_new_cases"].clip(lower=0)
    df["daily_new_deaths"] = df["raw_daily_new_deaths"].clip(lower=0)

    df["epi_year"] = df["date"].dt.isocalendar().year
    df["epi_week"] = df["date"].dt.isocalendar().week

    weekly = (
        df.groupby(["state", "fips", "epi_year", "epi_week"], as_index=False)
        .agg(
            week_start=("date", "min"),
            new_cases=("daily_new_cases", "sum"),
            new_deaths=("daily_new_deaths", "sum"),
            # "last", not "max": the source is cumulative and can be
            # revised downward mid-week (see had_correction), so the
            # officially-reported end-of-week total is whatever the state
            # reported on the last day of the week, not the highest value
            # seen — those differ exactly on a correction week.
            cumulative_cases_end=("cases", "last"),
            cumulative_deaths_end=("deaths", "last"),
            had_correction=("had_correction", "any"),
        )
    )
    weekly["week_start"] = weekly["week_start"].dt.date.astype(str)

    n_corrections = int(weekly["had_correction"].sum())
    logger.info(
        "Transformed %d daily rows -> %d state-week rows (%d weeks flagged with a data correction)",
        len(df), len(weekly), n_corrections,
    )
    return weekly
