# Get_Active_Tables.py
# Helper for debugging; ADF usually calls SP directly, but this can be used for manual checks

import pandas as pd

jdbc_url = dbutils.secrets.get(scope='azure-sql', key='control-db-jdbc-url')
jdbc_user = dbutils.secrets.get(scope='azure-sql', key='control-db-username')
jdbc_pass = dbutils.secrets.get(scope='azure-sql', key='control-db-password')

query = "SELECT * FROM control.table_metadata WHERE is_active = 1"

df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"({query}) as tmp") \
    .option("user", jdbc_user) \
    .option("password", jdbc_pass) \
    .load()

display(df)
