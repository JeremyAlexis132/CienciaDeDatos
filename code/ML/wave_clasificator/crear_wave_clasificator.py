import os
from pathlib import Path
import joblib
import pandas as pd
from sklearn.mixture import GaussianMixture
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
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
PORCENTAJE_SAMPLE_DATA = 1
EVERY_N_YEARS = 3
RANDOM_SEED = 0

SCALER_FEATURES = [
    'wind_speed_ms', 'wind_cos_direction', 'wind_sin_direction', 'wave_height_m', 
    'wave_cos_direction', 'wave_sin_direction', 'wave_period_s', 'wave_energy', 'wave_steepness'
]

MODEL_FEATURES = ['wind_speed_ms', 'wave_height_m', 'wave_period_s', 'wave_steepness']
SORT_FEATURES = ["wave_height_m", "wind_speed_ms", "wave_steepness"]
EXTREME_FEATURES = ['wind_speed_ms', 'wave_height_m']
N_CLUSTERS = 6

RANDOM_SEED_RANDOM_FOREST = 42
TEST_SIZE = 0.2
N_ESTIMATORS = 500
MIN_SAMPLES_LEAF = 10
N_JOBS = -1
TARGET = "sea_state_level"

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

    # Pre etiquetar con GMM (apredizaje no supervisado)
    gmm = GaussianMixture(
        n_components=N_CLUSTERS,
        covariance_type="full",
        random_state=RANDOM_SEED
    )
    data_sample["cluster"] = gmm.fit_predict(scaled_df[MODEL_FEATURES])
    
    cluster_summary = (
        data_sample.groupby("cluster")[MODEL_FEATURES]
        .mean()
        .sort_values(SORT_FEATURES)
    )
    cluster_order = {
        old_cluster: new_cluster + 1
        for new_cluster, old_cluster in enumerate(cluster_summary.index)
    }
    data_sample["sea_state_level"] = data_sample["cluster"].map(cluster_order)

    data_sample['mask_extremo'] = data_sample[EXTREME_FEATURES[0]] > data_sample[EXTREME_FEATURES[0]].quantile(0.99)
    for feature in EXTREME_FEATURES[1:]:
        data_sample['mask_extremo'] &= data_sample[feature] > data_sample[feature].quantile(0.99)
    data_sample.loc[data_sample['mask_extremo'], 'sea_state_level'] = 7

    # Entrenar clasificador Random Forest (aprendizaje supervisado)
    X = data_sample[MODEL_FEATURES]
    y = data_sample[TARGET].astype(int)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_SEED_RANDOM_FOREST,
        stratify=y
    )

    rf_classifier = RandomForestClassifier(
        n_estimators=N_ESTIMATORS,
        max_depth=None,
        min_samples_leaf=MIN_SAMPLES_LEAF,
        class_weight="balanced",
        random_state=RANDOM_SEED_RANDOM_FOREST,
        n_jobs=N_JOBS
    )
    rf_classifier.fit(X_train, y_train)

    y_pred = rf_classifier.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    balanced_accuracy = balanced_accuracy_score(y_test, y_pred)
    print("Accuracy:", accuracy)
    print("Balanced accuracy:", balanced_accuracy)
    print(
        classification_report(
            y_test,
            y_pred,
            digits=3
        )
    )
    cm = confusion_matrix(y_test, y_pred)
    print(cm)

    if (accuracy < 0.85) or (balanced_accuracy < 0.85):
        print(f'El modelo Random Forest para la costa {coast} no alcanzó el umbral de rendimiento.')
        continue

    model_path = model_path.format(coast)
    joblib.dump(rf_classifier, model_path)