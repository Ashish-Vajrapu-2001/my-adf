# Deployment Guide: CLV Metadata-Driven Framework

## 1. Prerequisites
- **Azure Resources**:
  - Azure SQL Database (Control DB)
  - Azure Data Factory
  - Azure Databricks
  - Azure Data Lake Gen2
- **Access**:
  - Service Principal for ADF to access Data Lake and SQL
  - Databricks Secret Scope created

## 2. SQL Setup
Execute scripts in this order on your Control Database:
1. `sql/control_tables/01_create_control_tables.sql`
2. `sql/control_tables/02_populate_control_tables.sql`
3. `sql/stored_procedures/sp_GetCDCChanges.sql`
4. `sql/stored_procedures/sp_UpdateTableMetadata.sql`
5. `sql/stored_procedures/sp_GetTableLoadOrder.sql`

## 3. Databricks Configuration
Create the secret scope and add credentials:
```bash
databricks secrets create-scope --scope azure-sql
databricks secrets put --scope azure-sql --key control-db-jdbc-url --string-value "jdbc:sqlserver://<server>.database.windows.net:1433;database=<db>"
databricks secrets put --scope azure-sql --key control-db-username --string-value "<username>"
databricks secrets put --scope azure-sql --key control-db-password --string-value "<password>"
```

Import notebooks from `databricks/notebooks/` to your workspace.

## 4. ADF Setup
1. Create Linked Services:
   - `LS_AzureSQL_Control`
   - `LS_AzureSQL_Dynamic` (Parameterized with `source_system_id`)
   - `LS_AzureDataLakeStorage`
   - `LS_AzureDatabricks`
2. Import Datasets (`adf/dataset/*.json`)
3. Import Pipelines (`adf/pipeline/*.json`)

## 5. Execution
Trigger `PL_Master_Orchestrator`. It will:
1. Identify tables requiring Initial Load.
2. Load them and register in Delta.
3. Identify tables ready for Incremental Load.
4. Process CDC changes dynamically.
