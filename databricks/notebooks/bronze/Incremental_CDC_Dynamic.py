# Incremental_CDC_Dynamic.py
# Handles UPSERT using Delta Merge

from pyspark.sql.functions import current_timestamp, lit, col
from delta.tables import *
import java.sql.DriverManager as DriverManager

# 1. Widgets
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system_id", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "") # Comma separated
dbutils.widgets.text("incremental_path", "")
dbutils.widgets.text("pipeline_run_id", "")

table_id = dbutils.widgets.get("table_id")
source_system_id = dbutils.widgets.get("source_system_id")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
pk_string = dbutils.widgets.get("primary_key_columns")
incremental_path = dbutils.widgets.get("incremental_path")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

# Paths
full_incremental_path = f"/mnt/datalake/{incremental_path}"
target_delta_path = f"/mnt/datalake/bronze/{source_system_id}/{schema_name}/{table_name}/delta"

# JDBC
jdbc_url = dbutils.secrets.get(scope='azure-sql', key='control-db-jdbc-url')
jdbc_user = dbutils.secrets.get(scope='azure-sql', key='control-db-username')
jdbc_pass = dbutils.secrets.get(scope='azure-sql', key='control-db-password')

try:
    # 2. Read Incremental Data
    source_df = spark.read.parquet(full_incremental_path)
    row_count = source_df.count()

    if row_count > 0:
        # Get Max Version
        max_version = source_df.agg({"_current_sync_version": "max"}).collect()[0][0]

        # 3. Prepare Merge Logic
        pk_cols = [x.strip() for x in pk_string.split(',')]

        # Construct dynamic merge condition: target.ID = source.ID AND target.ID2 = source.ID2
        merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_cols])

        # Define update/insert dictionaries (exclude metadata cols)
        # We need to exclude CDC specific columns from the SET clause
        cdc_metadata_cols = ["SYS_CHANGE_OPERATION", "SYS_CHANGE_VERSION", "_current_sync_version"]
        data_cols = [c for c in source_df.columns if c not in cdc_metadata_cols]

        # Build dictionary map for merge
        update_map = {col: f"source.{col}" for col in data_cols}
        update_map["_ingestion_timestamp"] = "current_timestamp()"
        update_map["_pipeline_run_id"] = f"'{pipeline_run_id}'"
        update_map["_is_current"] = "true"
        update_map["_is_deleted"] = "false"

        # For inserts, we use same map plus static metadata
        insert_map = update_map.copy()
        insert_map["_source_system_id"] = f"'{source_system_id}'"
        insert_map["_table_id"] = f"'{table_id}'"
        insert_map["_operation"] = "'I'"

        # 4. Execute Merge
        target_table = DeltaTable.forPath(spark, target_delta_path)

        target_table.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition = "source.SYS_CHANGE_OPERATION = 'U'",
            set = update_map
        ).whenMatchedUpdate(
            condition = "source.SYS_CHANGE_OPERATION = 'D'",
            set = {
                "_is_current": "false",
                "_is_deleted": "true",
                "_ingestion_timestamp": "current_timestamp()",
                "_pipeline_run_id": f"'{pipeline_run_id}'"
            }
        ).whenNotMatchedInsert(
            condition = "source.SYS_CHANGE_OPERATION != 'D'", # Don't insert if it's a delete on a non-existent record
            values = insert_map
        ).execute()

    else:
        max_version = None # No change

    # 5. Update Control
    if max_version is not None:
        connection = DriverManager.getConnection(jdbc_url, jdbc_user, jdbc_pass)
        statement = connection.createStatement()
        query = f"""
            EXEC control.sp_UpdateTableMetadata
               @TableId = '{table_id}',
               @LastSyncVersion = {max_version},
               @LoadStatus = 'success',
               @RowsProcessed = {row_count},
               @ErrorMessage = NULL,
               @IsInitialLoad = 0,
               @PipelineRunId = '{pipeline_run_id}'
        """
        statement.execute(query)
        connection.close()

    dbutils.notebook.exit(f'{{"status": "success", "rows": {row_count}}}')

except Exception as e:
    error_msg = str(e).replace("'", "")

    try:
        connection = DriverManager.getConnection(jdbc_url, jdbc_user, jdbc_pass)
        statement = connection.createStatement()
        statement.execute(f"""
            EXEC control.sp_UpdateTableMetadata
               @TableId = '{table_id}',
               @LastSyncVersion = NULL,
               @LoadStatus = 'failed',
               @RowsProcessed = 0,
               @ErrorMessage = '{error_msg}',
               @IsInitialLoad = 0,
               @PipelineRunId = '{pipeline_run_id}'
        """)
        connection.close()
    except:
        pass

    raise e
