"""
DAG: covid_surveillance_etl
============================
Weekly ETL of public COVID-19 state-level surveillance data into a
Postgres warehouse table, with a data-quality gate between extract and
transform and a post-load reconciliation check.

extract_us_states -> validate_raw -> transform_to_weekly -> load_to_warehouse -> reconcile_row_counts

Business logic lives in scripts/ as plain, unit-testable functions — this
file only wires them into Airflow tasks, sets scheduling/retry policy, and
passes data between tasks via XCom (small summary dicts, not full
dataframes — the dataframe itself is re-derived from the landed file in
each task that needs it, which is the standard pattern for anything
bigger than a few KB).
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from extract import extract_us_states
from validate import validate_us_states
from transform import transform_to_weekly
from load import load_weekly, row_count

logger = logging.getLogger(__name__)

WAREHOUSE_CONN_STR = "postgresql://postgres:postgres@localhost:5432/etl_warehouse"

default_args = {
    "owner": "rhutika.patil",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # would be True + alert email/Slack webhook in production
}


def _extract(**context):
    run_date = context["ds"]
    path = extract_us_states(run_date)
    context["ti"].xcom_push(key="landed_path", value=path)


def _validate(**context):
    path = context["ti"].xcom_pull(key="landed_path", task_ids="extract_us_states")
    result = validate_us_states(path)
    context["ti"].xcom_push(key="validation_result", value=result)


def _transform(**context):
    path = context["ti"].xcom_pull(key="landed_path", task_ids="extract_us_states")
    weekly = transform_to_weekly(path)
    staged_path = str(Path(path).parent / "weekly_transformed.parquet")
    weekly.to_parquet(staged_path, index=False)
    context["ti"].xcom_push(key="staged_path", value=staged_path)
    context["ti"].xcom_push(key="row_count", value=len(weekly))


def _load(**context):
    import pandas as pd
    staged_path = context["ti"].xcom_pull(key="staged_path", task_ids="transform_to_weekly")
    weekly = pd.read_parquet(staged_path)
    n_loaded = load_weekly(weekly, WAREHOUSE_CONN_STR)
    context["ti"].xcom_push(key="n_loaded", value=n_loaded)


def _reconcile(**context):
    expected = context["ti"].xcom_pull(key="row_count", task_ids="transform_to_weekly")
    actual = row_count(WAREHOUSE_CONN_STR)
    logger.info("Reconciliation: %d rows transformed this run, %d total rows now in warehouse table", expected, actual)
    if actual < expected:
        raise ValueError(f"Warehouse has fewer rows ({actual}) than this run alone transformed ({expected}) — load likely failed silently.")


with DAG(
    dag_id="covid_surveillance_etl",
    description="Weekly public COVID-19 state surveillance ETL into Postgres",
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-engineering", "public-health", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_us_states",
        python_callable=_extract,
        doc_md="Pull the raw NYT US-states COVID CSV and land it partitioned by run date.",
    )

    validate_task = PythonOperator(
        task_id="validate_raw",
        python_callable=_validate,
        doc_md="Schema/null/row-count/sanity checks on the landed file. Fails the run on any violation.",
    )

    transform_task = PythonOperator(
        task_id="transform_to_weekly",
        python_callable=_transform,
        doc_md="Diff cumulative counts to incident counts, flag corrections, aggregate to state-epiweek.",
    )

    load_task = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=_load,
        doc_md="Idempotent upsert into covid_state_weekly (Postgres).",
    )

    reconcile_task = PythonOperator(
        task_id="reconcile_row_counts",
        python_callable=_reconcile,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Post-load sanity check: warehouse row count can't be less than what this run loaded.",
    )

    extract_task >> validate_task >> transform_task >> load_task >> reconcile_task
