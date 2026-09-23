# Infrastructure-as-code reference for deploying this pipeline on AWS.
#
# NOT applied in this project's build environment — that would need a real
# AWS account and credentials, neither available in a sandboxed build
# container. This is provided as a precise deployment target: syntactically
# valid Terraform that maps every piece of the local pipeline to its AWS
# equivalent, ready to `terraform init && terraform plan` against a real
# account. See docs/aws_deployment.md for the narrative version and the
# GCP equivalents.
#
# Mapping:
#   local Postgres (etl_warehouse)  -> Amazon RDS for PostgreSQL
#   data/lake/raw/ landing zone      -> S3 landing bucket
#   `airflow dags test` / local scheduler -> Amazon MWAA (Managed Workflows for Apache Airflow)
#   DAG's requirements.txt            -> MWAA's requirements.txt (same file, S3-hosted)

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

variable "warehouse_db_password" {
  description = "Set via TF_VAR_warehouse_db_password or a secrets backend — never committed."
  type        = string
  sensitive   = true
}

# ---- S3 landing zone (replaces data/lake/raw/ on local disk) ----
resource "aws_s3_bucket" "landing" {
  bucket = "rhutika-covid-etl-landing"
}

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    id     = "expire-old-raw-landings"
    status = "Enabled"
    expiration {
      days = 90
    }
  }
}

# ---- RDS Postgres warehouse (replaces local Postgres) ----
resource "aws_db_subnet_group" "warehouse" {
  name       = "etl-warehouse-subnets"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "warehouse" {
  name_prefix = "etl-warehouse-"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Postgres from the MWAA environment's security group only"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.mwaa_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "warehouse" {
  identifier             = "etl-warehouse"
  engine                 = "postgres"
  engine_version         = "16"
  instance_class         = "db.t4g.micro" # right-size for real load; micro is a portfolio/demo footprint
  allocated_storage      = 20
  storage_encrypted      = true
  db_name                = "etl_warehouse"
  username               = "postgres"
  password               = var.warehouse_db_password
  db_subnet_group_name   = aws_db_subnet_group.warehouse.name
  vpc_security_group_ids = [aws_security_group.warehouse.id]
  skip_final_snapshot    = false
  final_snapshot_identifier = "etl-warehouse-final-snapshot"
  backup_retention_period   = 7
  deletion_protection       = true
}

variable "vpc_id" {}
variable "private_subnet_ids" {
  type = list(string)
}
variable "mwaa_security_group_id" {
  description = "Security group attached to the MWAA environment (see mwaa.tf sketch in docs/aws_deployment.md)"
}

output "warehouse_endpoint" {
  value = aws_db_instance.warehouse.endpoint
}

output "landing_bucket" {
  value = aws_s3_bucket.landing.bucket
}
