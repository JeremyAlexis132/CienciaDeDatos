import os

ambiente = os.getenv("DATABRICKS_BUNDLE_TARGET")

print(ambiente)

# Tablas de la capa Bronze
spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_dev.bronze.raw_swell_metrics (
            data STRING,
            source_file STRING,
            ingestion_timestamp TIMESTAMP
        ) USING DELTA
    """
)

# Tablas de la capa Silver
spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_dev.silver.swell_metrics (
            coast_name STRING,
            datetime TIMESTAMP,
            year INTEGER,
            wind_speed_ms DOUBLE,
            wind_direction_deg DOUBLE,
            wave_height_m DOUBLE,
            wave_direction_deg DOUBLE,
            wave_period_s DOUBLE,
            source_file STRING,
            ingestion_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (coast_name, year)
    """
)

spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_dev.silver.quarantine_swell_metrics (
            data STRING,
            source_file STRING,
            error_reason STRING,
            ingestion_timestamp TIMESTAMP
        )
        USING DELTA
    """
)

# Tablas de la capa Gold