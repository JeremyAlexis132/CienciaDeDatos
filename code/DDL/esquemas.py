import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS cor_{ambiente}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS cor_{ambiente}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS cor_{ambiente}.gold")