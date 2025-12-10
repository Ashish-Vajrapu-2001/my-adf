# Validate_Dependencies.py
# Checks for circular dependencies in control.load_dependencies

jdbc_url = dbutils.secrets.get(scope='azure-sql', key='control-db-jdbc-url')
jdbc_user = dbutils.secrets.get(scope='azure-sql', key='control-db-username')
jdbc_pass = dbutils.secrets.get(scope='azure-sql', key='control-db-password')

# Recursive CTE query to check depth and cycles
query = """
    WITH CTE AS (
        SELECT parent_table_id, child_table_id, 1 as level,
               CAST(parent_table_id + '->' + child_table_id AS VARCHAR(MAX)) as path
        FROM control.load_dependencies

        UNION ALL

        SELECT d.parent_table_id, d.child_table_id, c.level + 1,
               CAST(c.path + '->' + d.child_table_id AS VARCHAR(MAX))
        FROM control.load_dependencies d
        JOIN CTE c ON d.parent_table_id = c.child_table_id
        WHERE c.level < 20 -- Max depth safety
    )
    SELECT * FROM CTE WHERE level >= 20
"""

df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"({query}) as tmp") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .load()

if df.count() > 0:
    print("CRITICAL: Potential circular dependency detected!")
    display(df)
else:
    print("Dependency validation passed. No cycles detected.")
