import os
from pathlib import Path
import joblib
import pandas as pd
from sklearn.mixture import GaussianMixture
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score
)
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

RF_FEATURES = [
    'wind_speed_ms', 'wave_energy', 
    'wave_period_s', 'wave_steepness'
]
TARGET = "gmm_sea_state_level"
TEST_SIZE = 0.25
N_ESTIMATORS = 500
MAX_DEPTH = None
MIN_SAMPLES_LEAF = 10
CLASS_WEIGHT = "balanced"
N_JOBS = -1

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
    print(f'Procesando costa: {coast}')
    # obtener datos
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

    # Generar data sample
    coast_year_month_dict = {row.coast_year_month: PORCENTAJE_SAMPLE_DATA for row in data.select('coast_year_month').distinct().collect()}
    data_sample = (
        data
        .sampleBy('coast_year_month', fractions=coast_year_month_dict, seed=RANDOM_SEED)
        .drop('coast_year_month')
    ).toPandas()

    # Preparar datos
    scaler = joblib.load(scaler_path.format(coast))
    scaled_data = scaler.transform(data_sample[SCALER_FEATURES])
    scaled_df = pd.DataFrame(scaled_data, columns=SCALER_FEATURES)

    # Entrenar modelo GMM de clasificación no supervisada
    gmm = GaussianMixture(
        n_components=N_CLUSTERS,
        covariance_type="full",
        random_state=RANDOM_SEED
    )
    data_sample["gmm_cluster"] = gmm.fit_predict(scaled_df[GMM_FEATURES])
    data_sample["gmm_cluster_probability"] = gmm.predict_proba(scaled_df[GMM_FEATURES]).max(axis=1)
    
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

    # Evaluar modelo GMM)
    if not (
        all(data_sample.groupby('gmm_sea_state_level')['gmm_cluster_probability'].quantile(0.05)>=0.5) and
        all(data_sample.groupby('gmm_sea_state_level')['gmm_cluster_probability'].quantile(0.10)>=0.7) and
        all(data_sample.groupby('gmm_sea_state_level')['gmm_cluster_probability'].quantile(0.15)>=0.9) 
    ):
        print(f'El modelo GMM para la costa {coast} no es lo suficientemente bueno, saltando...')
        continue

    # Entrar modelo Random Forest de clasificación supervisada
    X = data_sample[RF_FEATURES]
    y = data_sample[TARGET].astype(int)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_SEED,
        stratify=y
    )

    rf_classifier = RandomForestClassifier(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        min_samples_leaf=MIN_SAMPLES_LEAF,
        class_weight=CLASS_WEIGHT,
        random_state=RANDOM_SEED,
        n_jobs=N_JOBS
    )

    rf_classifier.fit(X_train, y_train)

    y_pred = rf_classifier.predict(X_test)

    # Evaluar modelo
    acurracy = accuracy_score(y_test, y_pred)
    balanced_acurracy = balanced_accuracy_score(y_test, y_pred)
    if not(
        acurracy >= 0.7 and 
        balanced_acurracy >= 0.5
    ):
        print(f'El modelo Random Forest para la costa {coast} no es lo suficientemente bueno, saltando...')
        continue

    joblib.dump(rf_classifier, model_path.format(coast))