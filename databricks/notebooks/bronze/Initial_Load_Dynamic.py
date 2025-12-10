# Initial_Load_Dynamic.py
# Uses PySpark and Delta Lake to process initial load and register tables

from pyspark.sql.functions import current_timestamp, lit, col, expr
from delta.tables import *
import traceback

# 1. Get Widgets
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system_id", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("pipeline_run_id", "")

table_id = dbutils.widgets.get("table_id")
source_system_id = dbutils.widgets.get("source_system_id")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
source_path = dbutils.widgets.get("source_path")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

# Construct paths and table names
full_source_path = f"/mnt/datalake/{source_path}"
target_delta_path = f"/mnt/datalake/bronze/{source_system_id}/{schema_name}/{table_name}/delta"
delta_table_name = f"bronze.{source_system_id}_{schema_name}_{table_name}"

# JDBC Configuration (Secrets)
jdbc_url = dbutils.secrets.get(scope='azure-sql', key='control-db-jdbc-url')
jdbc_user = dbutils.secrets.get(scope='azure-sql', key='control-db-username')
jdbc_pass = dbutils.secrets.get(scope='azure-sql', key='control-db-password')
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

try:
    # 2. Read Parquet Source
    df = spark.read.parquet(full_source_path)

    # 3. Extract Sync Version
    # Assumes _sync_version column exists from query
    max_version = df.agg({"_sync_version": "max"}).collect()[0][0]
    row_count = df.count()

    # 4. Add Metadata Columns
    final_df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                 .withColumn("_source_system_id", lit(source_system_id)) \
                 .withColumn("_table_id", lit(table_id)) \
                 .withColumn("_operation", lit("I")) \
                 .withColumn("_is_current", lit(True)) \
                 .withColumn("_is_deleted", lit(False)) \
                 .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
                 .drop("_sync_version") # Persist separately

    # 5. Write to Delta
    final_df.write.format("delta").mode("overwrite").save(target_delta_path)

    # 6. Create Table if not exists
    spark.sql(f"CREATE TABLE IF NOT EXISTS {delta_table_name} USING DELTA LOCATION '{target_delta_path}'")

    # 7. Update Control Metadata
    update_query = f"""
        EXEC control.sp_UpdateTableMetadata
           @TableId = '{table_id}',
           @LastSyncVersion = {max_version},
           @LoadStatus = 'success',
           @RowsProcessed = {row_count},
           @ErrorMessage = NULL,
           @IsInitialLoad = 1,
           @PipelineRunId = '{pipeline_run_id}'
    """

    # Use JDBC to execute SP
    # PySpark JDBC doesn't support EXEC directly easily without dbtable hack or driver manager
    # Using specific JDBC execution pattern

    import java.sql.DriverManager as DriverManager
    connection = DriverManager.getConnection(jdbc_url, jdbc_user, jdbc_pass)
    statement = connection.createStatement()
    statement.execute(update_query)
    connection.close()

    dbutils.notebook.exit(f'{{"status": "success", "rows_processed": {row_count}, "sync_version": {max_version}}}')

except Exception as e:
    error_msg = str(e).replace("'", "")

    # Log failure to DB
    try:
        import java.sql.DriverManager as DriverManager
        connection = DriverManager.getConnection(jdbc_url, jdbc_user, jdbc_pass)
        statement = connection.createStatement()
        statement.execute(f"""
            EXEC control.sp_UpdateTableMetadata
               @TableId = '{table_id}',
               @LastSyncVersion = NULL,
               @LoadStatus = 'failed',
               @RowsProcessed = 0,
               @ErrorMessage = '{error_msg}',
               @IsInitialLoad = 1,
               @PipelineRunId = '{pipeline_run_id}'
        """)
        connection.close()
    except:
        print("Failed to log error to DB")

    raise e
