import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

spark.sql(f"DROP VOLUME IF EXISTS cor_{ambiente}.bronze.landing")