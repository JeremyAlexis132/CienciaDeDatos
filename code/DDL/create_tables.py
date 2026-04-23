import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

# Tablas de la capa Bronze
spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.bronze.raw_swell_metrics (
            data STRING,
            source_file STRING,
            ingestion_timestamp TIMESTAMP
        ) USING DELTA
    """
)

# Tablas de la capa Silver
spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.silver.swell_metrics (
            coast_name STRING,
            datetime TIMESTAMP,
            year INTEGER,
            wind_speed_ms FLOAT,
            wind_direction_deg FLOAT,
            wave_height_m FLOAT,
            wave_direction_deg FLOAT,
            wave_period_s FLOAT,
            source_file STRING,
            ingestion_timestamp TIMESTAMP,
            transformation_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (coast_name, year)
    """
)

spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.silver.quarantine_swell_metrics (
            data STRING,
            source_file STRING,
            error_reason STRING,
            ingestion_timestamp TIMESTAMP,
            transformation_timestamp TIMESTAMP
        )
        USING DELTA
    """
)

# Tablas de la capa Gold