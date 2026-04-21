import dlt
from pyspark.sql.functions import input_file_name, current_timestamp
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

@dlt.table(name="raw_swell_metrics")
def ingest_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(f"/Volumes/cor_{ambiente}/bronze/landing")
        .select(
            "value", # Columna con el renglón completo
            input_file_name().alias("source_file"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )