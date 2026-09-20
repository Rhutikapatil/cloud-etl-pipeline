# Cloud ETL Pipeline — Public Health Surveillance

An Apache Airflow pipeline that extracts real public COVID-19 state
surveillance data, validates it, transforms cumulative counts into
incident weekly counts, and loads it into a Postgres warehouse — with a
data-quality gate before transform and a reconciliation check after load.

## This actually ran — here's the log

```
extract_us_states     -> landed 2,217,680 bytes from a live public source
validate_raw           -> passed: 61,942 rows, 56 states, 2020-01-21 to 2023-03-23
transform_to_weekly    -> 61,942 daily rows -> 8,899 state-week rows (384 weeks flagged with a data correction)
load_to_warehouse      -> upserted 8,899 rows into covid_state_weekly (real Postgres)
reconcile_row_counts   -> 8,899 rows transformed this run, 8,899 total rows in warehouse — match
DagRun Finished: state=success
```

Sample of what landed in the warehouse:

```
     state      | epi_year | epi_week | week_start | new_cases | new_deaths
North Carolina  |     2020 |       10 | 2020-03-03 |         2 |          0
North Carolina  |     2020 |       11 | 2020-03-09 |        30 |          0
North Carolina  |     2020 |       12 | 2020-03-16 |       237 |          0
North Carolina  |     2020 |       13 | 2020-03-23 |       897 |          6
```

3 unit tests pass against the transform logic (`pytest tests/`), including
a regression test for a real bug caught during development — see below.

## Data source

Real, public data: the [NYT COVID-19 US states time series](https://github.com/nytimes/covid-19-data)
(`us-states.csv`), pulled live over HTTPS at DAG run time — not a static
file bundled with this repo. It's genuinely cumulative daily case/death
counts per state, including the messy real-world property that matters
most for this project: **states periodically revise their cumulative
totals downward** (removing duplicate or misattributed cases), which
breaks the naive "today minus yesterday" diff if you don't handle it.

## The bug this project caught (and why it's here)

The first version of the weekly aggregation computed each state's
end-of-week cumulative total with `MAX(cases)` over the week. That's
wrong on a correction week: if a state's cumulative count peaks mid-week
and is then revised down, `MAX()` silently reports the pre-correction
(inflated) number as the week's ending total. The fix uses the
*chronologically last* value instead — what the state actually reported
as of the end of the week — and a regression test
(`tests/test_transform.py::test_cumulative_end_of_week_uses_last_day_not_max`)
locks that in. Leaving this in the README instead of quietly fixing it is
deliberate: catching this kind of thing is the actual job.

## Pipeline

```
extract_us_states → validate_raw → transform_to_weekly → load_to_warehouse → reconcile_row_counts
```

| Stage | What it does |
|---|---|
| `extract_us_states` | Pulls the raw CSV, lands it unmodified at a path partitioned by run date (mirrors an S3 landing-zone key structure) |
| `validate_raw` | Schema, null, row-count, and state-count checks — raises and fails the run rather than passing bad data downstream |
| `transform_to_weekly` | Diffs cumulative → incident counts per day, flags (doesn't hide) negative corrections, aggregates to state/epi-week |
| `load_to_warehouse` | Idempotent upsert (`ON CONFLICT ... DO UPDATE`) into Postgres, keyed on state + epi-week — safe to re-run or backfill |
| `reconcile_row_counts` | Confirms the warehouse table actually grew by what this run transformed |

Business logic lives in `scripts/` as plain, unit-testable functions —
the DAG file (`dags/covid_surveillance_etl.py`) only wires them into
Airflow tasks and sets scheduling/retry policy.

## Running it

**As run in this build** (no Docker daemon available in that sandbox —
Airflow installed directly via pip, against a locally installed Postgres):

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

# Postgres warehouse (adjust for your system's package manager)
sudo apt-get install -y postgresql && sudo -u postgres createdb etl_warehouse

export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
airflow db migrate
airflow dags test covid_surveillance_etl 2026-09-14
```

**With Docker** (the easier path anywhere Docker is available):
```bash
docker compose up airflow-init
docker compose up
# Airflow UI at localhost:8080 (admin/admin), DAG runs on its @weekly schedule
```

**Tests:**
```bash
pytest tests/
```

**On AWS:** see `docs/aws_deployment.md` and `infra/main.tf` — S3 landing
bucket, RDS Postgres, and Amazon MWAA, mapped 1:1 to the local pieces
above. Not applied in this build (no AWS account/credentials available in
the build environment), but written to `terraform plan` cleanly against a
real one.

## Project structure

```
.
├── dags/
│   └── covid_surveillance_etl.py   # DAG definition — thin, wires scripts/ into tasks
├── scripts/
│   ├── extract.py
│   ├── validate.py
│   ├── transform.py                 # includes the correction-handling logic above
│   └── load.py
├── tests/
│   └── test_transform.py            # 3 tests, incl. the MAX-vs-last regression test
├── infra/
│   └── main.tf                      # AWS Terraform reference (S3 + RDS + security groups)
├── docs/
│   └── aws_deployment.md            # narrative deployment guide, AWS + GCP mapping
├── docker-compose.yml                # local Airflow + Postgres, Docker-based
└── requirements.txt
```

## Stack

Python · Apache Airflow · PostgreSQL · pandas · Terraform (AWS) · Docker

## Author

Rhutika Patil — M.S. Bioinformatics, NC State University
[linkedin.com/in/rhutika-patil](https://linkedin.com/in/rhutika-patil) ·
[github.com/Rhutikapatil](https://github.com/Rhutikapatil)
