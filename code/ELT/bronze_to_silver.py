from pyspark.sql.functions import (
    current_date, current_timestamp, col, to_timestamp, 
    concat, lit, split, trim, lower, substring_index, size, when, floor
)
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente

df_bronze_raw_swell_metrics = (
    spark.readStream
        .format("delta")
        .table(f"cor_{ambiente}.bronze.raw_swell_metrics")
)

df_bronze_raw_swell_metrics_split_cols = (
    df_bronze_raw_swell_metrics.withColumn(
        "cols", 
        split(trim(col("data")), r"\s+")
    )
)

df_bronze_raw_swell_metrics_pre_transform = (
    df_bronze_raw_swell_metrics_split_cols.withColumn(
        "flagg_passed_struct_check", 
        size(col("cols")) == 12
    )
)

df_bronze_raw_swell_metrics_transformed = (
    df_bronze_raw_swell_metrics_pre_transform.filter(
        col("flagg_passed_struct_check") == True
    ).select(
        lower(
            substring_index(
                substring_index(
                    col("source_file"), 
                    "/", 
                    -1
                ), 
                ".", 
                1
            )
        ).alias("coast_name"),
        to_timestamp(concat(
            col("cols")[11].cast("double").cast("int"), lit("-"),
            col("cols")[0].cast("double").cast("int"), lit("-"),
            col("cols")[1].cast("double").cast("int"), lit(" "),
            col("cols")[2].cast("double").cast("int")
        ), "yyyy-M-d H").alias("datetime"),
        col("cols")[11].cast("double").cast("int").alias("year"),
        (floor(col("cols")[3].cast("double") * 100) / 100).cast("float").alias("wind_speed_ms"),
        (floor(col("cols")[4].cast("double") * 100) / 100).cast("float").alias("wind_direction_deg"),
        (floor(col("cols")[5].cast("double") * 100) / 100).cast("float").alias("wave_height_m"),
        (floor(col("cols")[6].cast("double") * 100) / 100).cast("float").alias("wave_direction_deg"),
        (floor(col("cols")[7].cast("double") * 100) / 100).cast("float").alias("wave_period_s"),
        col("source_file"),
        col("ingestion_timestamp"),
        current_timestamp().alias("transformation_timestamp"),
        col("data")
    )
)

df_bronze_raw_swell_metrics_quality = (
    df_bronze_raw_swell_metrics_transformed.withColumn(
        "flagg_passed_datetime_check",
        (col("datetime").isNotNull()) &
        (col("datetime") >= "1950-01-01") &
        (col("datetime") <= current_date())
    ).withColumn(
        "flagg_passed_wind_speed_ms_checks",
        (col("wind_speed_ms").between(0, 100))
    ).withColumn(
        "flagg_passed_wind_direction_deg_checks",
        (col("wind_direction_deg").between(0, 360))
    ).withColumn(
        "flagg_passed_wave_height_m_checks",
        (col("wave_height_m").between(0, 50))
    ).withColumn(
        "flagg_passed_wave_direction_deg_checks",
        (col("wave_direction_deg").between(0, 360))
    ).withColumn(
        "flagg_passed_wave_period_s_checks",
        (col("wave_period_s").between(0, 100))
    ).withColumn(
        "flagg_passed_quality_checks",
        (col("flagg_passed_datetime_check")) &
        (col("flagg_passed_wind_speed_ms_checks")) &
        (col("flagg_passed_wind_direction_deg_checks")) &
        (col("flagg_passed_wave_height_m_checks")) &
        (col("flagg_passed_wave_direction_deg_checks")) &
        (col("flagg_passed_wave_period_s_checks"))
    )
)

df_silver_swell_metrics = df_bronze_raw_swell_metrics_quality.filter(col("flagg_passed_quality_checks") == True) \
    .drop(
        "data", "flagg_passed_datetime_check", "flagg_passed_wind_speed_ms_checks",
        "flagg_passed_wind_direction_deg_checks", "flagg_passed_wave_height_m_checks", "flagg_passed_wave_direction_deg_checks",
        "flagg_passed_wave_period_s_checks", "flagg_passed_quality_checks"
    )

(
    df_silver_swell_metrics.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option("checkpointLocation", f"/Volumes/cor_{ambiente}/silver/data/checkpoints/swell_metrics")
        .toTable(f"cor_{ambiente}.silver.swell_metrics")
)

df_quarantine_swell_metrics = (
    df_bronze_raw_swell_metrics_pre_transform.filter(
        col("flagg_passed_struct_check") == False
    ).select(
        col("data"),
        col("source_file"),
        lit("Error Estructura: La fila no tiene el número correcto de columnas").alias("error_reason"),
        col("ingestion_timestamp"),
        current_timestamp().alias("transformation_timestamp")
    )
).unionByName(
    (
        df_bronze_raw_swell_metrics_quality.filter(
            col("flagg_passed_quality_checks") == False
        ).select(
            col("data"),
            col("source_file"),
            concat(
                when(
                    ~col("flagg_passed_datetime_check"), 
                    lit("Fecha invalida; ")
                ).otherwise(lit("")),
                when(
                    ~col("flagg_passed_wind_speed_ms_checks"), 
                    lit("Viento fuera de rango; ")
                ).otherwise(lit("")),
                when(
                    ~col("flagg_passed_wind_direction_deg_checks"), 
                    lit("Dir. Viento fuera de rango; ")
                ).otherwise(lit("")),
                when(
                    ~col("flagg_passed_wave_height_m_checks"), 
                    lit("Altura ola fuera de rango; ")
                ).otherwise(lit("")),
                when(
                    ~col("flagg_passed_wave_direction_deg_checks"), 
                    lit("Dir. Ola fuera de rango; ")
                ).otherwise(lit("")),
                when(
                    ~col("flagg_passed_wave_period_s_checks"), 
                    lit("Periodo ola fuera de rango; ")
                ).otherwise(lit(""))
            ).alias("error_reason"),
            col("ingestion_timestamp"),
            col("transformation_timestamp")
        )
    )
)

(
    df_quarantine_swell_metrics.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option("checkpointLocation", f"/Volumes/cor_{ambiente}/silver/data/checkpoints/quarantine_swell_metrics")
        .toTable(f"cor_{ambiente}.silver.quarantine_swell_metrics")
)