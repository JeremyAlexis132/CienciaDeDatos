import argparse
from pyspark.dbutils import DBUtils

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

spark.sql(
    f"""
        CREATE VOLUME IF NOT EXISTS cor_{ambiente}.bronze.data
    """
)
volume_path = f"/Volumes/cor_{ambiente}/bronze/data/landing"
dbutils = DBUtils(spark)
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
        CREATE VOLUME IF NOT EXISTS cor_{ambiente}.ML.models
    """
)

volume_path = f"/Volumes/cor_{ambiente}/ML/Models/wave_clasificator"
dbutils.fs.mkdirs(volume_path)