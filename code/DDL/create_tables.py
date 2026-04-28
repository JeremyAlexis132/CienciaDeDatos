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
            data STRING
                COMMENT '',
            source_file STRING
                COMMENT '',
            ingestion_timestamp TIMESTAMP
                COMMENT ''
        ) USING DELTA
            COMMENT ''
    """
)

# Tablas de la capa Silver
spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.silver.swell_metrics (
            id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
                COMMENT '',
            coast_name STRING
                COMMENT '',
            datetime TIMESTAMP
                COMMENT '',
            year INTEGER
                COMMENT '',
            wind_speed_ms FLOAT
                COMMENT '',
            wind_direction_deg FLOAT
                COMMENT '',
            wind_u FLOAT
                COMMENT '',
            wind_v FLOAT
                COMMENT '',
            wave_height_m FLOAT
                COMMENT '',
            wave_direction_deg FLOAT
                COMMENT '',
            wave_u FLOAT
                COMMENT '',
            wave_v FLOAT
                COMMENT '',
            wave_period_s FLOAT
                COMMENT '',
            ingestion_timestamp TIMESTAMP
                COMMENT '',
            source_file STRING
                COMMENT '',
            transformation_timestamp TIMESTAMP
                COMMENT ''
        )
        USING DELTA
        PARTITIONED BY (coast_name, year)
            COMMENT ''
    """
)

spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.silver.quarantine_swell_metrics (
            data STRING
                COMMENT '',
            source_file STRING
                COMMENT '',
            error_reason STRING
                COMMENT '',
            ingestion_timestamp TIMESTAMP
                COMMENT '',
            transformation_timestamp TIMESTAMP
                COMMENT ''
        )
        USING DELTA
            COMMENT ''
    """
)

# Tablas de la capa Gold