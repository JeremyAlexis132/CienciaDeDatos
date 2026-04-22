from pyspark.sql.functions import current_timestamp, col
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.useIncrementalListing", "true")
        .load(f"/Volumes/cor_{ambiente}/bronze/data/landing/")
)

df_transformed = df.select(
    col("value").alias("data"),
    col("_metadata.file_path").alias("source_file"),
    current_timestamp().alias("ingestion_timestamp")
)

(
    df_transformed.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option("checkpointLocation", f"/Volumes/cor_{ambiente}/bronze/data/checkpoints/raw_swell_metrics")
        .toTable(f"cor_{ambiente}.bronze.raw_swell_metrics")
)