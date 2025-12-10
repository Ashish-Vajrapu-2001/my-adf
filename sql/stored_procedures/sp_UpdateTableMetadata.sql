CREATE PROCEDURE control.sp_UpdateTableMetadata
    @TableId VARCHAR(20),
    @LastSyncVersion BIGINT,
    @LoadStatus VARCHAR(20),
    @RowsProcessed BIGINT,
    @ErrorMessage NVARCHAR(MAX) = NULL,
    @IsInitialLoad BIT = 0,
    @PipelineRunId VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now DATETIME2 = GETDATE();
    DECLARE @RetryCount INT;
    DECLARE @MaxRetries INT;

    -- Get current retry status
    SELECT @RetryCount = retry_count, @MaxRetries = max_retries
    FROM control.table_metadata
    WHERE table_id = @TableId;

    IF @LoadStatus = 'success'
    BEGIN
        -- Update metadata on success
        UPDATE control.table_metadata
        SET
            last_sync_version = @LastSyncVersion,
            last_sync_timestamp = @Now,
            last_load_status = 'success',
            last_load_end_time = @Now,
            last_load_rows_processed = @RowsProcessed,
            last_error_message = NULL,
            retry_count = 0, -- Reset retries on success
            initial_load_completed = CASE WHEN @IsInitialLoad = 1 THEN 1 ELSE initial_load_completed END,
            last_modified_date = @Now
        WHERE table_id = @TableId;
    END
    ELSE
    BEGIN
        -- Update metadata on failure
        UPDATE control.table_metadata
        SET
            last_load_status = 'failed',
            last_load_end_time = @Now,
            last_error_message = @ErrorMessage,
            retry_count = ISNULL(retry_count, 0) + 1,
            is_active = CASE WHEN (ISNULL(retry_count, 0) + 1) >= max_retries THEN 0 ELSE 1 END, -- Disable if max retries exceeded
            last_modified_date = @Now
        WHERE table_id = @TableId;
    END

    -- Log execution
    INSERT INTO control.pipeline_execution_log
    (
        pipeline_run_id,
        table_id,
        operation_type,
        execution_end_time,
        execution_status,
        rows_written,
        sync_version_end,
        error_message
    )
    VALUES
    (
        @PipelineRunId,
        @TableId,
        CASE WHEN @IsInitialLoad = 1 THEN 'INITIAL' ELSE 'INCREMENTAL' END,
        @Now,
        @LoadStatus,
        @RowsProcessed,
        @LastSyncVersion,
        @ErrorMessage
    );
END;
GO
