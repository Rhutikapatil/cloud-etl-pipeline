"""
Unit tests for the transform logic — run these without Airflow at all
(`pytest tests/`), which is the point of keeping business logic in plain
functions under scripts/ rather than inline in the DAG.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from transform import transform_to_weekly


@pytest.fixture
def sample_csv(tmp_path):
    # Two states, one week, including a deliberate downward correction
    # (Testland's cumulative cases drop from day 3 to day 4) to verify the
    # correction-flagging behavior rather than silently clipping it away.
    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"] * 2,
        "state": ["Testland"] * 4 + ["Otherland"] * 4,
        "fips": [1] * 4 + [2] * 4,
        "cases": [100, 110, 130, 125, 50, 60, 65, 70],  # Testland corrects down on day 4
        "deaths": [1, 1, 2, 2, 0, 0, 1, 1],
    })
    path = tmp_path / "us-states.csv"
    df.to_csv(path, index=False)
    return str(path)


def test_aggregates_to_one_row_per_state_week(sample_csv):
    weekly = transform_to_weekly(sample_csv)
    assert len(weekly) == 2  # both states' 4 days fall in the same ISO week
    assert set(weekly["state"]) == {"Testland", "Otherland"}


def test_flags_correction_without_hiding_it(sample_csv):
    weekly = transform_to_weekly(sample_csv)
    testland = weekly[weekly.state == "Testland"].iloc[0]
    otherland = weekly[weekly.state == "Otherland"].iloc[0]

    assert testland["had_correction"] == True  # noqa: E712 — day 4 diff is -5
    assert otherland["had_correction"] == False

    # new_cases is clipped to zero for the corrected day, so weekly total is
    # 100 (day1, no prior) + 10 + 20 + 0 = 130, not the naive 125 (final-first)
    assert testland["new_cases"] == 130


def test_cumulative_end_of_week_uses_last_day_not_max(sample_csv):
    # Regression test for the correction-week bug: end-of-week cumulative
    # must be the LAST reported value (125, after the correction), not the
    # highest value seen mid-week (130, from before the correction).
    weekly = transform_to_weekly(sample_csv)
    testland = weekly[weekly.state == "Testland"].iloc[0]
    assert testland["cumulative_cases_end"] == 125
