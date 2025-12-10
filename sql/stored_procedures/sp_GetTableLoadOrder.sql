CREATE PROCEDURE control.sp_GetTableLoadOrder
AS
BEGIN
    SET NOCOUNT ON;

    -- Recursive CTE to calculate topological depth based on dependencies
    WITH TableDepth AS (
        -- Anchor: Tables with no parents
        SELECT
            t.table_id,
            0 AS depth
        FROM control.table_metadata t
        WHERE NOT EXISTS (
            SELECT 1 FROM control.load_dependencies d
            WHERE d.child_table_id = t.table_id
        ) AND t.is_active = 1

        UNION ALL

        -- Recursive member: Children of processed tables
        SELECT
            d.child_table_id,
            td.depth + 1
        FROM TableDepth td
        JOIN control.load_dependencies d ON td.table_id = d.parent_table_id
        JOIN control.table_metadata t ON d.child_table_id = t.table_id
        WHERE t.is_active = 1
    )
    SELECT
        tm.table_id,
        tm.source_system_id,
        tm.schema_name,
        tm.table_name,
        tm.primary_key_columns,
        tm.is_composite_key,
        tm.initial_load_completed,
        ISNULL(tm.last_sync_version, -1) as last_sync_version,
        -- Priority: Explicit priority first, then depth (dependency order)
        ROW_NUMBER() OVER (ORDER BY tm.load_priority ASC, MAX(td.depth) ASC) as execution_order
    FROM control.table_metadata tm
    LEFT JOIN TableDepth td ON tm.table_id = td.table_id
    WHERE tm.is_active = 1
    GROUP BY
        tm.table_id, tm.source_system_id, tm.schema_name, tm.table_name,
        tm.primary_key_columns, tm.is_composite_key, tm.initial_load_completed,
        tm.last_sync_version, tm.load_priority
    ORDER BY execution_order;
END;
GO
