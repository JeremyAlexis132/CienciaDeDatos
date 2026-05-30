import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

spark.sql(f"DROP TABLE IF EXISTS cor_{ambiente}.bronze.raw_swell_metrics")
spark.sql(f"DROP TABLE IF EXISTS cor_{ambiente}.silver.swell_metrics")
spark.sql(f"DROP TABLE IF EXISTS cor_{ambiente}.silver.quarantine_swell_metrics")
spark.sql(f"DROP TABLE IF EXISTS cor_{ambiente}.silver.swell_clasification")
spark.sql(f"DROP TABLE IF EXISTS cor_{ambiente}.gold.wave_daily_summary")
spark.sql(f"DROP TABLE IF EXISTS cor_{ambiente}.gold.wave_monthly_classification")
spark.sql(f"DROP TABLE IF EXISTS cor_{ambiente}.gold.significant_wave_height_forecast")