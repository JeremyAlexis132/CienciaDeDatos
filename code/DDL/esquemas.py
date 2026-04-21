import os

ambiente = os.getenv("DATABRICKS_BUNDLE_TARGET")

print(ambiente)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS cor_{ambiente}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS cor_{ambiente}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS cor_{ambiente}.gold")