from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

df_silver_swell_metrics = spark.sql(
    f"""
        SELECT s_m.*
        FROM cor_{ambiente}.silver.swell_metrics s_m
        LEFT JOIN cor_{ambiente}.gold.wave_daily_summary d_s
            ON DATE(s_m.datetime) = d_s.date
        WHERE d_s.date IS NULL;
    """
)

df_silver_swell_metrics_transformed = (
    df_silver_swell_metrics
    .groupBy(
        F.window("datetime", "1 day").alias("window"),
        "coast_name"
    ).agg(
        F.max("wave_height_m").alias("max_wave_height_m"),
        F.expr("max_by(wave_period_s, wave_height_m)").alias("max_wave_period_s"),
        F.expr("max_by(wave_direction_deg, wave_height_m)").alias("max_wave_direction_deg"),
        F.expr("max_by(wind_speed_ms, wave_height_m)").alias("max_wave_wind_speed_ms"),
        F.expr("max_by(wind_direction_deg, wave_height_m)").alias("max_wave_wind_direction_deg"),
        F.min("wave_height_m").alias("min_wave_height_m"),
        F.expr("min_by(wave_period_s, wave_height_m)").alias("min_wave_period_s"),
        F.expr("min_by(wave_direction_deg, wave_height_m)").alias("min_wave_direction_deg"),
        F.expr("min_by(wind_speed_ms, wave_height_m)").alias("min_wave_wind_speed_ms"),
        F.expr("min_by(wind_direction_deg, wave_height_m)").alias("min_wave_wind_direction_deg"),
        F.avg("wave_height_m").alias("avg_wave_height_m")
    )
)

df_gold_wave_daily_summary = (
    df_silver_swell_metrics_transformed
    .select(
        F.to_date(F.col("window.start")).alias("date"),
        F.col("coast_name"),
        F.col("max_wave_height_m").cast("float"),
        F.col("max_wave_period_s").cast("float"),
        F.col("max_wave_direction_deg").cast("float"),
        F.col("max_wave_wind_speed_ms").cast("float"),
        F.col("max_wave_wind_direction_deg").cast("float"),
        F.col("min_wave_height_m").cast("float"),
        F.col("min_wave_period_s").cast("float"),
        F.col("min_wave_direction_deg").cast("float"),
        F.col("min_wave_wind_speed_ms").cast("float"),
        F.col("min_wave_wind_direction_deg").cast("float"),
        F.col("avg_wave_height_m").cast("float")
    )
)

(
    df_gold_wave_daily_summary
        .write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"cor_{ambiente}.gold.wave_daily_summary")
    
)