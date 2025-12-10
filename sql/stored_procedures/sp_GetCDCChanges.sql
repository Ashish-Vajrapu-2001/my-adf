CREATE PROCEDURE control.sp_GetCDCChanges
    @SchemaName NVARCHAR(50),
    @TableName NVARCHAR(100),
    @PrimaryKeyColumns NVARCHAR(500), -- 'COL1' or 'COL1,COL2'
    @LastSyncVersion BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @CurrentVersion BIGINT = CHANGE_TRACKING_CURRENT_VERSION();
    DECLARE @JoinCondition NVARCHAR(MAX) = '';

    -- Step 1 & 2: Parse comma-separated primary keys and build dynamic JOIN condition
    -- We use string manipulation to build the join clause: CT.PK = T.PK AND CT.PK2 = T.PK2

    SELECT @JoinCondition = STRING_AGG(
        CAST('CT.' + QUOTENAME(TRIM(value)) + ' = T.' + QUOTENAME(TRIM(value)) AS NVARCHAR(MAX)),
        ' AND '
    )
    FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Validation: Ensure we generated a valid join condition
    IF @JoinCondition IS NULL OR @JoinCondition = ''
    BEGIN
        RAISERROR('Could not build join condition. Check PrimaryKeyColumns parameter.', 16, 1);
        RETURN;
    END

    -- Step 3: Build dynamic SQL using CHANGETABLE
    -- We select metadata columns from CT and all columns from the actual table T
    -- We use LEFT JOIN to handle Deletes (where T.* would be NULL, but we still need the key from CT)

    SET @SQL = '
        SELECT
            CT.SYS_CHANGE_OPERATION,
            CT.SYS_CHANGE_VERSION,
            ' + CAST(@CurrentVersion AS NVARCHAR(20)) + ' as _current_sync_version,
            CT.*, -- Include PKs from Change Table explicitly for Deletes
            T.*   -- Include data columns
        FROM CHANGETABLE(CHANGES ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) +
             ', ' + CAST(@LastSyncVersion AS NVARCHAR(20)) + ') AS CT
        LEFT JOIN ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' AS T
        ON ' + @JoinCondition + '
        WHERE CT.SYS_CHANGE_VERSION <= ' + CAST(@CurrentVersion AS NVARCHAR(20));

    -- Debug print (optional, commented out for production)
    -- PRINT @SQL;

    -- Step 4: Execute dynamic SQL
    EXEC sp_executesql @SQL;
END;
GO
