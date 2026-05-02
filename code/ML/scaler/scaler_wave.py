from sklearn.preprocessing import StandardScaler
import joblib
from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)
PORCENTAJE_ENTRENAMIENTO = 0.2

data = (
    spark.sql(
        f"""
            SELECT coast_name, datetime, wind_u, wind_v, wave_u, wave_v, wave_period_s
            FROM cor_{ambiente}.silver.swell_metrics
        """
    )
)

data_pre_processing= (
    data
    .withColumn('coast_year_month', F.concat(F.col('coast_name'), F.lit('_'), F.date_format('datetime', 'yyyy-MM')))
)

coast_year_month_dict = {row.coast_year_month: PORCENTAJE_ENTRENAMIENTO for row in data_pre_processing.select('coast_year_month').distinct().collect()}
data_sample = (
    data_pre_processing
    .sampleBy('coast_year_month', fractions=coast_year_month_dict, seed=0)
    .drop('coast_year_month')
).toPandas()

features = ['wind_u', 'wind_v', 'wave_u', 'wave_v', 'wave_period_s']

scaler = StandardScaler()
X = scaler.fit_transform(data_sample[features])

path = f'/Volumes/cor_{ambiente}/ml/models/scaler/scaler.pkl'
joblib.dump(scaler, path)