import os
from pathlib import Path
import joblib
import pandas as pd
from sklearn.mixture import GaussianMixture
import argparse

# Argumentos
parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()
ambiente = args.ambiente
print(ambiente)

# Parametros
PORCENTAJE_SAMPLE_DATA = 0.7
EVERY_N_YEARS = 2
RANDOM_SEED = 0

SCALER_FEATURES = [
    'wind_speed_ms', 'wind_cos_direction', 'wind_sin_direction', 'wave_height_m', 
    'wave_cos_direction', 'wave_sin_direction', 'wave_period_s', 'wave_energy', 'wave_steepness'
]

GMM_FEATURES = [
    'wind_speed_ms', 'wave_energy', 
    'wave_period_s', 'wave_steepness'
]
EXTREME_FEATURES = ['wind_speed_ms', 'wave_energy', 'wave_period_s']
SORT_FEATURES = ["wave_energy", "wave_period_s", "wind_speed_ms"]
N_CLUSTERS = 6
EXTREME_THRESHOLD = 0.9

if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
    scaler_path = f'/Volumes/cor_{ambiente}/ml/models/scaler/scaler_{{}}.pkl'
    model_path = f'/Volumes/cor_{ambiente}/ml/models/wave_clasificator/wave_clasificator_{{}}.pkl'
else:
    base_path = Path.cwd().parent
    scaler_path = f'{base_path}/scaler/scaler_{{}}.pkl'
    model_path = f'{base_path}/wave_clasificator/wave_clasificator_{{}}.pkl'

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
    print('-' * 50)
    print(f'Procesando costa: {coast}')
    data = (
        spark.sql(
            f"""
                SELECT coast_name, datetime, {', '.join(SCALER_FEATURES)},
                CONCAT(coast_name, '_', DATE_FORMAT(datetime, 'yyyyMM')) AS coast_year_month
                FROM cor_{ambiente}.silver.swell_metrics
                WHERE coast_name = '{coast}'
                AND YEAR(datetime) % {EVERY_N_YEARS} = 0
            """
        )
    )

    if data.count() == 0:
        print(f'No hay datos para la costa {coast}, saltando...')
        continue

    coast_year_month_dict = {row.coast_year_month: PORCENTAJE_SAMPLE_DATA for row in data.select('coast_year_month').distinct().collect()}
    data_sample = (
        data
        .sampleBy('coast_year_month', fractions=coast_year_month_dict, seed=RANDOM_SEED)
        .drop('coast_year_month')
    ).toPandas()

    scaler_path = scaler_path.format(coast)
    scaler = joblib.load(scaler_path)
    scaled_data = scaler.transform(data_sample[SCALER_FEATURES])
    scaled_df = pd.DataFrame(scaled_data, columns=SCALER_FEATURES)

    gmm = GaussianMixture(
        n_components=N_CLUSTERS,
        covariance_type="full",
        random_state=RANDOM_SEED
    )
    data_sample["gmm_cluster"] = gmm.fit_predict(scaled_df[GMM_FEATURES])
    
    cluster_summary = (
        data_sample.groupby("gmm_cluster")[GMM_FEATURES]
        .mean()
        .sort_values(SORT_FEATURES)
    )
    cluster_order = {
        old_cluster: new_cluster + 1
        for new_cluster, old_cluster in enumerate(cluster_summary.index)
    }

    data_sample["gmm_sea_state_level"] = data_sample["gmm_cluster"].map(cluster_order)

    data_sample['gmm_mask_extremo'] = data_sample[EXTREME_FEATURES[0]] > data_sample[EXTREME_FEATURES[0]].quantile(EXTREME_THRESHOLD)
    for feature in EXTREME_FEATURES[1:]:
        data_sample['gmm_mask_extremo'] &= data_sample[feature] > data_sample[feature].quantile(EXTREME_THRESHOLD)

    data_sample.loc[data_sample['gmm_mask_extremo'], 'gmm_sea_state_level'] = 7

    model_path = model_path.format(coast)
    joblib.dump(gmm, model_path)