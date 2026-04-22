import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

spark.sql(f"DROP SCHEMA IF EXISTS cor_{ambiente}.bronze CASCADE")
spark.sql(f"DROP SCHEMA IF EXISTS cor_{ambiente}.silver CASCADE")
spark.sql(f"DROP SCHEMA IF EXISTS cor_{ambiente}.gold CASCADE")