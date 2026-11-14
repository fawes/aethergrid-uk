# AetherGrid UK — Multi-Cloud Ready Terraform IaC (AWS UK London eu-west-2 Reference)
# Equivalent Mappings:
#   aws_s3_bucket        <-> google_storage_bucket (GCS)      <-> azurerm_storage_account (ADLS Gen2)
#   aws_glue_catalog_db  <-> google_bigquery_dataset (BigLake)<-> azurerm_synapse_workspace
#   aws_ecs_cluster      <-> google_cloud_run_v2_service      <-> azurerm_container_app

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-2"
}

resource "aws_s3_bucket" "iceberg_lakehouse" {
  bucket = "aethergrid-uk-prod-iceberg-lakehouse"
}

resource "aws_glue_catalog_database" "energy_lakehouse" {
  name = "aethergrid_uk_energy_lakehouse"
}

resource "aws_ecs_cluster" "decision_api_cluster" {
  name = "aethergrid-uk-decision-engine"
}
