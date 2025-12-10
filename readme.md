# CLV Metadata-Driven CDC Framework

A complete, production-ready framework for ingesting Customer Lifetime Value data from multiple Azure SQL sources into a Databricks Lakehouse using a metadata-driven approach.

## Key Features
- **100% Metadata Driven**: No hardcoded table names in pipelines.
- **Dynamic CDC**: Uses `CHANGETABLE` and `STRING_SPLIT` to handle any primary key combination (single or composite).
- **Resilient**: Automatic retries, error logging, and dependency management.
- **Secure**: Uses Databricks Secrets and Key Vault.

## Architecture
- **Control DB**: SQL Server tables managing metadata, state, and logs.
- **ADF**: Orchestrator loop that dispatches jobs based on Control DB state.
- **Databricks**: PySpark logic for generic Initial Load and generic CDC Merge.

## Supported Tables
The system is pre-configured with metadata for:
- CRM (Customers, Incidents, Interactions...)
- ERP (Orders, Lines, Products, City Tiers...)
- Marketing (Campaigns)

## Handling Composite Keys
The system automatically detects composite keys (e.g., `ERP.CITY_TIER_MASTER` uses `CITY,STATE`). The stored procedure `sp_GetCDCChanges` dynamically builds the join condition, and the Databricks notebook dynamically builds the Delta MERGE condition.

## Adding New Tables
1. Enable Change Tracking on the source table.
2. Insert a row into `control.table_metadata`.
3. If it has dependencies, add rows to `control.load_dependencies`.
