from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

df_bronze_raw_swell_metrics = (
    spark.readStream
        .format("delta")
        .table(f"cor_{ambiente}.bronze.raw_swell_metrics")
)

df_bronze_raw_swell_metrics_split_cols = (
    df_bronze_raw_swell_metrics.withColumn(
        "cols", 
        F.split(F.trim(F.col("data")), r"\s+")
    )
)

df_bronze_raw_swell_metrics_pre_transform = (
    df_bronze_raw_swell_metrics_split_cols.withColumn(
        "number_of_columns", F.size(F.col("cols"))
    ).withColumn(
        "flagg_passed_struct_check", 
        F.size(F.col("cols")).isin(12, 13)
    )
)

df_bronze_raw_swell_metrics_transformed = (
    df_bronze_raw_swell_metrics_pre_transform.filter(
        (F.col("flagg_passed_struct_check") == True) &
        (F.col("number_of_columns") == 12)
    ).select(
        F.lower(
            F.substring_index(
                F.substring_index(
                    F.col("source_file"), 
                    "/", 
                    -1
                ), 
                ".", 
                1
            )
        ).alias("coast_name"),
        F.to_timestamp(F.concat(
            F.col("cols")[11].cast("double").cast("int"), F.lit("-"),
            F.col("cols")[0].cast("double").cast("int"), F.lit("-"),
            F.col("cols")[1].cast("double").cast("int"), F.lit(" "),
            F.col("cols")[2].cast("double").cast("int")
        ), "yyyy-M-d H").alias("datetime"),
        F.col("cols")[11].cast("double").cast("int").alias("year"),
        F.round(F.col("cols")[3], 2).cast("float").alias("wind_speed_ms"),
        F.round(F.col("cols")[4], 2).cast("float").alias("wind_direction_deg"),
        F.round(F.col("cols")[5], 2).cast("float").alias("wave_height_m"),
        F.round(F.col("cols")[6], 2).cast("float").alias("wave_direction_deg"),
        F.round(F.col("cols")[7], 2).cast("float").alias("wave_period_s"),
        F.col("ingestion_timestamp"),
        F.col("source_file"),
        F.current_timestamp().alias("transformation_timestamp"),
        F.col("data")
    )
).unionByName(
    df_bronze_raw_swell_metrics_pre_transform.filter(
        (F.col("flagg_passed_struct_check") == True) &
        (F.col("number_of_columns") == 13)
    ).select(
        F.lower(
            F.substring_index(
                F.substring_index(
                    F.col("source_file"), 
                    "/", 
                    -1
                ), 
                ".", 
                1
            )
        ).alias("coast_name"),
        F.to_timestamp(F.concat(
            F.col("cols")[12].cast("double").cast("int"), F.lit("-"),
            F.col("cols")[0].cast("double").cast("int"), F.lit("-"),
            F.col("cols")[1].cast("double").cast("int"), F.lit(" "),
            F.col("cols")[2].cast("double").cast("int")
        ), "yyyy-M-d H").alias("datetime"),
        F.col("cols")[12].cast("double").cast("int").alias("year"),
        F.round(F.col("cols")[3], 2).cast("float").alias("wind_speed_ms"),
        F.round(F.col("cols")[4], 2).cast("float").alias("wind_direction_deg"),
        F.round(F.col("cols")[5], 2).cast("float").alias("wave_height_m"),
        F.round(F.col("cols")[6], 2).cast("float").alias("wave_direction_deg"),
        F.round(F.col("cols")[7], 2).cast("float").alias("wave_period_s"),
        F.col("ingestion_timestamp"),
        F.col("source_file"),
        F.current_timestamp().alias("transformation_timestamp"),
        F.col("data")
    )
)
df_bronze_raw_swell_metrics_add_u_v = (
    df_bronze_raw_swell_metrics_transformed.withColumn(
        "wind_u", F.round(F.col("wind_speed_ms") * F.sin(F.radians(F.col("wind_direction_deg"))), 2).alias("wind_u")
    ).withColumn(
        "wind_v", F.round(F.col("wind_speed_ms") * F.cos(F.radians(F.col("wind_direction_deg"))), 2).alias("wind_v")
    ).withColumn(
        "wave_u", F.round(F.col("wave_height_m") * F.sin(F.radians(F.col("wave_direction_deg"))), 2).alias("wave_u")
    ).withColumn(
        "wave_v", F.round(F.col("wave_height_m") * F.cos(F.radians(F.col("wave_direction_deg"))), 2).alias("wave_v")
    )
)

df_bronze_raw_swell_metrics_quality = (
    df_bronze_raw_swell_metrics_add_u_v.withColumn(
        "flagg_passed_datetime_check",
        (F.col("datetime").isNotNull()) &
        (F.col("datetime") >= "1979-01-01") &
        (F.col("datetime") < "2019-01-01")
    ).withColumn(
        "flagg_passed_wind_speed_ms_checks",
        (F.col("wind_speed_ms").between(0, 100))
    ).withColumn(
        "flagg_passed_wind_direction_deg_checks",
        (F.col("wind_direction_deg").between(0, 360))
    ).withColumn(
        "flagg_passed_wave_height_m_checks",
        (F.col("wave_height_m").between(0, 50))
    ).withColumn(
        "flagg_passed_wave_direction_deg_checks",
        (F.col("wave_direction_deg").between(0, 360))
    ).withColumn(
        "flagg_passed_wave_period_s_checks",
        (F.col("wave_period_s").between(0, 100))
    ).withColumn(
        "flagg_passed_quality_checks",
        (F.col("flagg_passed_datetime_check")) &
        (F.col("flagg_passed_wind_speed_ms_checks")) &
        (F.col("flagg_passed_wind_direction_deg_checks")) &
        (F.col("flagg_passed_wave_height_m_checks")) &
        (F.col("flagg_passed_wave_direction_deg_checks")) &
        (F.col("flagg_passed_wave_period_s_checks"))
    )
)

df_silver_swell_metrics = df_bronze_raw_swell_metrics_quality.filter(F.col("flagg_passed_quality_checks") == True) \
    .drop(
        "data", "flagg_passed_datetime_check", "flagg_passed_wind_speed_ms_checks",
        "flagg_passed_wind_direction_deg_checks", "flagg_passed_wave_height_m_checks", "flagg_passed_wave_direction_deg_checks",
        "flagg_passed_wave_period_s_checks", "flagg_passed_quality_checks"
    )

df_quarantine_swell_metrics = (
    df_bronze_raw_swell_metrics_pre_transform.filter(
        F.col("flagg_passed_struct_check") == False
    ).select(
        F.col("data"),
        F.col("source_file"),
        F.lit("Error Estructura: La fila no tiene el número correcto de columnas").alias("error_reason"),
        F.col("ingestion_timestamp"),
        F.current_timestamp().alias("transformation_timestamp"),
        F.lit(False).alias("resolved")
    )
).unionByName(
    (
        df_bronze_raw_swell_metrics_quality.filter(
            F.col("flagg_passed_quality_checks") == False
        ).select(
            F.col("data"),
            F.col("source_file"),
            F.concat(
                F.when(
                    ~F.col("flagg_passed_datetime_check"), 
                    F.lit("Fecha invalida; ")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col("flagg_passed_wind_speed_ms_checks"), 
                    F.lit("Viento fuera de rango; ")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col("flagg_passed_wind_direction_deg_checks"), 
                    F.lit("Dir. Viento fuera de rango; ")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col("flagg_passed_wave_height_m_checks"), 
                    F.lit("Altura ola fuera de rango; ")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col("flagg_passed_wave_direction_deg_checks"), 
                    F.lit("Dir. Ola fuera de rango; ")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col("flagg_passed_wave_period_s_checks"), 
                    F.lit("Periodo ola fuera de rango; ")
                ).otherwise(F.lit(""))
            ).alias("error_reason"),
            F.col("ingestion_timestamp"),
            F.col("transformation_timestamp"),
            F.lit(False).alias("resolved")
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