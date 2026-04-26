from pyspark.sql.functions import (
    current_date, current_timestamp, col, to_timestamp, 
    concat, lit, split, trim, lower, substring_index, size, when, round, sin, cos, radians
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
        "number_of_columns", size(col("cols"))
    ).withColumn(
        "flagg_passed_struct_check", 
        size(col("cols")).isin(12, 13)
    )
)

df_bronze_raw_swell_metrics_transformed = (
    df_bronze_raw_swell_metrics_pre_transform.filter(
        (col("flagg_passed_struct_check") == True) &
        (col("number_of_columns") == 12)
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
        round(col("cols")[3], 2).cast("float").alias("wind_speed_ms"),
        round(col("cols")[4], 2).cast("float").alias("wind_direction_deg"),
        round(col("cols")[5], 2).cast("float").alias("wave_height_m"),
        round(col("cols")[6], 2).cast("float").alias("wave_direction_deg"),
        round(col("cols")[7], 2).cast("float").alias("wave_period_s"),
        col("ingestion_timestamp"),
        col("source_file"),
        current_timestamp().alias("transformation_timestamp"),
        col("data")
    )
).unionByName(
    df_bronze_raw_swell_metrics_pre_transform.filter(
        (col("flagg_passed_struct_check") == True) &
        (col("number_of_columns") == 13)
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
            col("cols")[12].cast("double").cast("int"), lit("-"),
            col("cols")[0].cast("double").cast("int"), lit("-"),
            col("cols")[1].cast("double").cast("int"), lit(" "),
            col("cols")[2].cast("double").cast("int")
        ), "yyyy-M-d H").alias("datetime"),
        col("cols")[12].cast("double").cast("int").alias("year"),
        round(col("cols")[3], 2).cast("float").alias("wind_speed_ms"),
        round(col("cols")[4], 2).cast("float").alias("wind_direction_deg"),
        round(col("cols")[5], 2).cast("float").alias("wave_height_m"),
        round(col("cols")[6], 2).cast("float").alias("wave_direction_deg"),
        round(col("cols")[7], 2).cast("float").alias("wave_period_s"),
        col("ingestion_timestamp"),
        col("source_file"),
        current_timestamp().alias("transformation_timestamp"),
        col("data")
    )
)
df_bronze_raw_swell_metrics_add_u_v = (
    df_bronze_raw_swell_metrics_transformed.withColumn(
        "wind_u", round(col("wind_speed_ms") * sin(radians(col("wind_direction_deg"))), 2).alias("wind_u")
    ).withColumn(
        "wind_v", round(col("wind_speed_ms") * cos(radians(col("wind_direction_deg"))), 2).alias("wind_v")
    ).withColumn(
        "wave_u", round(col("wave_height_m") * sin(radians(col("wave_direction_deg"))), 2).alias("wave_u")
    ).withColumn(
        "wave_v", round(col("wave_height_m") * cos(radians(col("wave_direction_deg"))), 2).alias("wave_v")
    )
)

df_bronze_raw_swell_metrics_quality = (
    df_bronze_raw_swell_metrics_add_u_v.withColumn(
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
    df_silver_swell_metrics.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option("checkpointLocation", f"/Volumes/cor_{ambiente}/silver/data/checkpoints/swell_metrics")
        .toTable(f"cor_{ambiente}.silver.swell_metrics")
)

(
    df_quarantine_swell_metrics.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option("checkpointLocation", f"/Volumes/cor_{ambiente}/silver/data/checkpoints/quarantine_swell_metrics")
        .toTable(f"cor_{ambiente}.silver.quarantine_swell_metrics")
)