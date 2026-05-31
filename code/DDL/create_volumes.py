import argparse
from pyspark.dbutils import DBUtils

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)
dbutils = DBUtils(spark)

spark.sql(
    f"""
        CREATE VOLUME IF NOT EXISTS cor_{ambiente}.bronze.data
    """
)
volume_path = f"/Volumes/cor_{ambiente}/bronze/data/landing/swell"
dbutils.fs.mkdirs(volume_path)
volume_path = f"/Volumes/cor_{ambiente}/bronze/data/landing/climate"
dbutils.fs.mkdirs(volume_path)

spark.sql(
    f"""
        CREATE VOLUME IF NOT EXISTS cor_{ambiente}.silver.data
    """
)

spark.sql(
    f"""
        CREATE VOLUME IF NOT EXISTS cor_{ambiente}.gold.data
    """
)

spark.sql(
    f"""
        CREATE VOLUME IF NOT EXISTS cor_{ambiente}.ml.models
    """
)

volume_path = f"/Volumes/cor_{ambiente}/ml/models/wave_classifier"
dbutils.fs.mkdirs(volume_path)