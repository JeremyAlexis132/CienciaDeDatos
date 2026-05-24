import argparse
from pyspark.dbutils import DBUtils

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)
dbutils = DBUtils(spark)

volume_path = f"/Volumes/cor_{ambiente}/bronze/data/checkpoints"
try:
    dbutils.fs.rm(volume_path, recurse=True)
except Exception as e:
    print(f"Error occurred while removing checkpoint directory: {e}")

volume_path = f"/Volumes/cor_{ambiente}/silver/data/checkpoints"
try:
    dbutils.fs.rm(volume_path, recurse=True)
except Exception as e:
    print(f"Error occurred while removing checkpoint directory: {e}")

volume_path = f"/Volumes/cor_{ambiente}/gold/data/checkpoints"
try:
    dbutils.fs.rm(volume_path, recurse=True)
except Exception as e:
    print(f"Error occurred while removing checkpoint directory: {e}")

volume_path = f"/Volumes/cor_{ambiente}/silver/data/checkpoints/swell_clasification/"
try:
    dbutils.fs.rm(volume_path, recurse=True)
except Exception as e:
    print(f"Error occurred while removing checkpoint directory: {e}")