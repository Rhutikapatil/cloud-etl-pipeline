"""
Load stage.

Idempotent upsert into the warehouse: re-running the pipeline for a date
range that's already loaded updates rows in place rather than duplicating
them (keyed on state + epi_year + epi_week) — important because Airflow
backfills and retries mean a task can legitimately run more than once for
the same logical date.

Target here is local Postgres (matches `data & databases: PostgreSQL` on
the author's stack). In a cloud deployment this connects to RDS Postgres
or Snowflake instead — only the connection string changes; the SQL is
ANSI-standard aside from the `ON CONFLICT` upsert clause, which has a
direct Snowflake equivalent (`MERGE INTO`).
"""

import logging

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS covid_state_weekly (
    state                   TEXT NOT NULL,
    fips                    INTEGER,
    epi_year                INTEGER NOT NULL,
    epi_week                INTEGER NOT NULL,
    week_start              DATE NOT NULL,
    new_cases               BIGINT NOT NULL,
    new_deaths              BIGINT NOT NULL,
    cumulative_cases_end    BIGINT NOT NULL,
    cumulative_deaths_end   BIGINT NOT NULL,
    had_correction          BOOLEAN NOT NULL,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (state, epi_year, epi_week)
);
"""

UPSERT = """
INSERT INTO covid_state_weekly
    (state, fips, epi_year, epi_week, week_start, new_cases, new_deaths,
     cumulative_cases_end, cumulative_deaths_end, had_correction, loaded_at)
VALUES %s
ON CONFLICT (state, epi_year, epi_week) DO UPDATE SET
    fips = EXCLUDED.fips,
    week_start = EXCLUDED.week_start,
    new_cases = EXCLUDED.new_cases,
    new_deaths = EXCLUDED.new_deaths,
    cumulative_cases_end = EXCLUDED.cumulative_cases_end,
    cumulative_deaths_end = EXCLUDED.cumulative_deaths_end,
    had_correction = EXCLUDED.had_correction,
    loaded_at = now();
"""


def load_weekly(df: pd.DataFrame, conn_str: str) -> int:
    conn = psycopg2.connect(conn_str)
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        rows = [
            (
                r.state, int(r.fips) if pd.notna(r.fips) else None, int(r.epi_year), int(r.epi_week),
                r.week_start, int(r.new_cases), int(r.new_deaths),
                int(r.cumulative_cases_end), int(r.cumulative_deaths_end), bool(r.had_correction),
                pd.Timestamp.utcnow(),
            )
            for r in df.itertuples(index=False)
        ]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=1000)
        conn.commit()
        logger.info("Upserted %d rows into covid_state_weekly", len(rows))
        return len(rows)
    finally:
        conn.close()


def row_count(conn_str: str, table: str = "covid_state_weekly") -> int:
    conn = psycopg2.connect(conn_str)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            return cur.fetchone()[0]
    finally:
        conn.close()
