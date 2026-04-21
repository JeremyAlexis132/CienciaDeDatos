import argparse
from pyspark.dbutils import DBUtils

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

spark.sql(
    f"""
        CREATE VOLUME IF NOT EXISTS cor_{ambiente}.bronze.landing
    """
)

volume_path = f"/Volumes/cor_{ambiente}/bronze/landing/data"
dbutils = DBUtils(spark)

dbutils.fs.mkdirs(volume_path)