-- Create Control Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]')
END
GO

-- 1. Source Systems
IF OBJECT_ID('control.source_systems', 'U') IS NOT NULL DROP TABLE control.source_systems;
CREATE TABLE control.source_systems (
    source_system_id VARCHAR(20) NOT NULL,
    system_name VARCHAR(100) NOT NULL,
    system_description VARCHAR(500),
    server_name VARCHAR(255),
    database_name VARCHAR(128),
    is_active BIT DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_source_systems PRIMARY KEY (source_system_id)
);

-- 2. Table Metadata (Master Control Table)
IF OBJECT_ID('control.table_metadata', 'U') IS NOT NULL DROP TABLE control.table_metadata;
CREATE TABLE control.table_metadata (
    table_id VARCHAR(20) NOT NULL,
    source_system_id VARCHAR(20) NOT NULL,
    schema_name NVARCHAR(50) NOT NULL,
    table_name NVARCHAR(100) NOT NULL,
    entity_name NVARCHAR(255),
    primary_key_columns NVARCHAR(500) NOT NULL,
    is_composite_key BIT DEFAULT 0,
    load_type VARCHAR(20) DEFAULT 'incremental',
    load_frequency VARCHAR(50),
    load_priority INT DEFAULT 100,
    is_active BIT DEFAULT 1,
    initial_load_completed BIT DEFAULT 0,
    last_sync_version BIGINT,
    last_sync_timestamp DATETIME2,
    last_load_status VARCHAR(20),
    last_load_start_time DATETIME2,
    last_load_end_time DATETIME2,
    last_load_rows_processed BIGINT,
    last_error_message NVARCHAR(MAX),
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    created_date DATETIME2 DEFAULT GETDATE(),
    last_modified_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_table_metadata PRIMARY KEY (table_id),
    CONSTRAINT FK_table_metadata_source_systems FOREIGN KEY (source_system_id)
        REFERENCES control.source_systems (source_system_id)
);

-- 3. Load Dependencies
IF OBJECT_ID('control.load_dependencies', 'U') IS NOT NULL DROP TABLE control.load_dependencies;
CREATE TABLE control.load_dependencies (
    dependency_id INT IDENTITY(1,1) NOT NULL,
    parent_table_id VARCHAR(20) NOT NULL,
    child_table_id VARCHAR(20) NOT NULL,
    dependency_type VARCHAR(20) DEFAULT 'REQUIRED',
    CONSTRAINT PK_load_dependencies PRIMARY KEY (dependency_id),
    CONSTRAINT FK_load_dependencies_parent FOREIGN KEY (parent_table_id)
        REFERENCES control.table_metadata (table_id),
    CONSTRAINT FK_load_dependencies_child FOREIGN KEY (child_table_id)
        REFERENCES control.table_metadata (table_id),
    CONSTRAINT CHK_no_self_dependency CHECK (parent_table_id <> child_table_id)
);

-- 4. Pipeline Execution Log
IF OBJECT_ID('control.pipeline_execution_log', 'U') IS NOT NULL DROP TABLE control.pipeline_execution_log;
CREATE TABLE control.pipeline_execution_log (
    execution_id BIGINT IDENTITY(1,1) NOT NULL,
    pipeline_run_id VARCHAR(100),
    table_id VARCHAR(20) NOT NULL,
    operation_type VARCHAR(20),
    execution_start_time DATETIME2 DEFAULT GETDATE(),
    execution_end_time DATETIME2,
    execution_status VARCHAR(20),
    rows_read BIGINT,
    rows_written BIGINT,
    sync_version_start BIGINT,
    sync_version_end BIGINT,
    error_message NVARCHAR(MAX),
    adf_pipeline_name VARCHAR(200),
    databricks_notebook_name VARCHAR(200),
    execution_duration_seconds INT,
    CONSTRAINT PK_pipeline_execution_log PRIMARY KEY (execution_id),
    CONSTRAINT FK_pipeline_execution_log_table FOREIGN KEY (table_id)
        REFERENCES control.table_metadata (table_id)
);

-- 5. Data Quality Rules
IF OBJECT_ID('control.data_quality_rules', 'U') IS NOT NULL DROP TABLE control.data_quality_rules;
CREATE TABLE control.data_quality_rules (
    rule_id VARCHAR(20) NOT NULL,
    table_id VARCHAR(20) NOT NULL,
    rule_name VARCHAR(200),
    rule_type VARCHAR(50),
    column_name NVARCHAR(128),
    rule_expression NVARCHAR(MAX),
    threshold_percent DECIMAL(5,2) DEFAULT 100.00,
    action_on_fail VARCHAR(20) DEFAULT 'WARN',
    severity VARCHAR(20) DEFAULT 'Medium',
    is_active BIT DEFAULT 1,
    CONSTRAINT PK_data_quality_rules PRIMARY KEY (rule_id),
    CONSTRAINT FK_dq_rules_table FOREIGN KEY (table_id)
        REFERENCES control.table_metadata (table_id)
);

-- Indexes for performance
CREATE INDEX IX_table_metadata_priority ON control.table_metadata(load_priority, is_active);
CREATE INDEX IX_execution_log_table_id ON control.pipeline_execution_log(table_id);
CREATE INDEX IX_load_dependencies_child ON control.load_dependencies(child_table_id);
GO
