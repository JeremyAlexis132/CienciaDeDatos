import os

ambiente = os.getenv("DATABRICKS_BUNDLE_TARGET")
print(ambiente)

spark.sql(
    f"""
        CREATE VOLUME IF NOT EXISTS cor_{ambiente}.bronze.landing
    """
)