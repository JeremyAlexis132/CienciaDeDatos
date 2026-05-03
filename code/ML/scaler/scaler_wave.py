from sklearn.preprocessing import StandardScaler
import joblib
import os
from pathlib import Path
from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)
PORCENTAJE_ENTRENAMIENTO = 0.5

data = (
    spark.sql(
        f"""
            SELECT coast_name, datetime, wind_u, wind_v, wave_u, wave_v, wave_period_s
            FROM cor_{ambiente}.silver.swell_metrics
        """
    )
)

data_pre_processing = (
    data
    .withColumn('coast_year_month', F.concat(F.col('coast_name'), F.lit('_'), F.date_format('datetime', 'yyyy-MM')))
)

coast_year_month_dict = {row.coast_year_month: PORCENTAJE_ENTRENAMIENTO for row in data_pre_processing.select('coast_year_month').distinct().collect()}

data_sample = (
    data_pre_processing
    .sampleBy('coast_year_month', fractions=coast_year_month_dict, seed=0)
    .drop('coast_year_month')
).toPandas()

coast_names = data_sample['coast_name'].unique()

features = ['wind_u', 'wind_v', 'wave_u', 'wave_v', 'wave_period_s']

scaler = StandardScaler()

if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
    scaler_path = f'/Volumes/cor_{ambiente}/ml/models/scaler/scaler_{{}}.pkl'
else:
    base_path = Path.cwd().parent
    scaler_path = f'{base_path}/scaler/scaler_{{}}.pkl'

for coast in coast_names:
    X = data_sample[data_sample['coast_name'] == coast][features]
    scaler.fit(X)
    joblib.dump(scaler, scaler_path.format(coast))