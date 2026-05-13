from sklearn.preprocessing import StandardScaler
import joblib
import os
from pathlib import Path
import argparse

# Argumentos
parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()
ambiente = args.ambiente
print(ambiente)

# Parametros
PORCENTAJE_SAMPLE_DATA = 1
EVERY_N_YEARS = 2
FEATURES = [
    'wind_speed_ms', 'wind_cos_direction', 'wind_sin_direction', 'wave_height_m', 
    'wave_cos_direction', 'wave_sin_direction', 'wave_period_s', 'wave_energy', 'wave_steepness'
]
RANDOM_SEED = 0

if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
    scaler_path = f'/Volumes/cor_{ambiente}/ml/models/scaler/scaler_{{}}.pkl'
else:
    scaler_path = f'{Path.cwd().parent}/scaler/scaler_{{}}.pkl'

# Obtener costas
coast_names = (
    spark.sql(
        f"""
            SELECT DISTINCT coast_name
            FROM cor_{ambiente}.silver.swell_metrics
        """
    )
).toPandas()['coast_name'].tolist()

for coast in coast_names:
    data = (
        spark.sql(
            f"""
                SELECT coast_name, datetime, {', '.join(FEATURES)},
                CONCAT(coast_name, '_', DATE_FORMAT(datetime, 'yyyyMM')) AS coast_year_month
                FROM cor_{ambiente}.silver.swell_metrics
                WHERE coast_name = '{coast}'
                AND YEAR(datetime) % {EVERY_N_YEARS} = 0
            """
        )
    )

    coast_year_month_dict = {row.coast_year_month: PORCENTAJE_SAMPLE_DATA for row in data.select('coast_year_month').distinct().collect()}
    data_sample = (
        data
        .sampleBy('coast_year_month', fractions=coast_year_month_dict, seed=RANDOM_SEED)
        .drop('coast_year_month')
    ).toPandas()

    scaler = StandardScaler()
    X = data_sample[FEATURES]
    scaler.fit(X)

    joblib.dump(scaler, scaler_path.format(coast))