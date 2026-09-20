# Deploying this pipeline on AWS (and the GCP equivalent)

This pipeline runs today against local Postgres (verified — see the main
README for the actual run log). This doc is the deployment path to make
it run the same way in the cloud, piece by piece.

## AWS mapping

| Local piece (this repo, as built) | AWS equivalent | Where |
|---|---|---|
| `data/lake/raw/` landing directory | S3 bucket, versioned, 90-day lifecycle expiry on raw landings | `infra/main.tf` |
| Local Postgres (`etl_warehouse`) | RDS for PostgreSQL, private subnet, encrypted at rest | `infra/main.tf` |
| `airflow dags test` / local scheduler | **Amazon MWAA** (Managed Workflows for Apache Airflow) — same DAG file, no code changes | sketch below |
| Airflow connection to warehouse | RDS credentials in **AWS Secrets Manager**, referenced via an Airflow `Connection` backed by the Secrets Manager secrets backend | — |
| `requirements.txt` | MWAA's own `requirements.txt`, uploaded to the same S3 bucket MWAA reads its DAGs from | — |

### Why S3 + RDS + MWAA specifically

- **S3** for the landing zone because raw extracts should be immutable,
  versioned, and cheap to retain — exactly what a landing bucket is for,
  and it decouples "did the extract succeed" from "did the transform
  succeed," so a failed transform can be retried against the same raw file
  without re-hitting the source.
- **RDS Postgres** rather than Redshift/Snowflake because the target
  table here (`covid_state_weekly`, ~9K rows) is well within
  transactional-database territory — this isn't an analytical workload
  that needs a columnar warehouse yet. If the panel grew to
  billions of rows or needed heavy analytical joins, Snowflake (the
  author's other listed data-warehouse tool) would be the next step, and
  the load SQL in `scripts/load.py` ports over almost unchanged (`ON
  CONFLICT` → `MERGE INTO`).
- **MWAA** over self-hosting Airflow on EC2/ECS because it removes
  scheduler/webserver ops entirely — you upload `dags/` and
  `requirements.txt` to S3 and MWAA runs them; for a portfolio-scale
  pipeline, the ops overhead of self-hosting isn't worth it.

### MWAA environment sketch

```hcl
resource "aws_mwaa_environment" "airflow" {
  name              = "covid-etl-mwaa"
  airflow_version   = "2.10.5"
  source_bucket_arn = aws_s3_bucket.landing.arn   # reuse the landing bucket's parent, or a dedicated one
  dag_s3_path       = "dags"
  execution_role_arn = aws_iam_role.mwaa_execution.arn

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids          = var.private_subnet_ids
  }

  environment_class = "mw1.small" # right-sizes to the real DAG's task count
}
```

(Full IAM role/policy and networking resources omitted here for length —
`infra/main.tf` has the warehouse and landing-zone resources that are
this pipeline's actual data-plane; the MWAA/IAM resources above are a
sketch of the remaining piece, not applied.)

## GCP equivalent

| AWS | GCP |
|---|---|
| S3 landing bucket | Cloud Storage bucket |
| RDS Postgres | Cloud SQL for PostgreSQL |
| MWAA | Cloud Composer (managed Airflow) |
| Secrets Manager | Secret Manager |

The DAG code and SQL are cloud-agnostic — only the connection strings and
the `provider` block in Terraform change.

## What's actually verified vs. what's a reference

- **Verified, ran end-to-end in this build**: extract from a real public
  data source over HTTPS → validate → transform → load into real
  Postgres → reconcile. See the main README's run log.
- **Reference, not applied**: the `infra/main.tf` Terraform and the MWAA
  sketch above — deploying them needs a real AWS account and credentials
  that weren't available in this build environment. They're written to
  `terraform plan` cleanly against a real account, not to be taken on
  faith.
