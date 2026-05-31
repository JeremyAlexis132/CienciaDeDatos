import os
from pathlib import Path
import joblib
import pandas as pd
from sklearn.preprocessing import StandardScaler
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
EXTREME_THRESHOLD = 0.95
EVERY_N_YEARS = 3
PORCENTAJE_SAMPLE_DATA_MONTH = 1
RANDOM_SEED = 0
TEST_SIZE = 0.2

N_CLUSTERS = 6
EXTREME_FEATURES = ['wave_energy', 'wave_power_kW_m']
MODEL_FEATURES = [
    'wind_speed_ms', 
    'wave_energy', 'wave_height_m', 'wave_period_s', 'wave_power_kW_m'
]
SCALED_MODEL_FEATURES = [f'{feature}_scaled' for feature in MODEL_FEATURES]

N_ESTIMATORS = 500
MAX_DEPTH = None
MIN_SAMPLES_LEAF = 10
CLASS_WEIGHT = "balanced"
N_JOBS = -1

if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
    model_path = f'/Volumes/cor_{ambiente}/ml/models/wave_classifier/wave_classifier.pkl'
else:
    base_path = Path.cwd().parent
    model_path = f'{base_path}/wave_classifier/wave_classifier.pkl'

def clasificar_gmm(df):
    def es_extremo(X):
        return (
            (X["wave_energy"] >= threshold_energia) &
            (X["wave_power_kW_m"] >= threshold_potencia)
        )
    def procesar(X):
        x = X.copy()
        x["es_extremo"] = es_extremo(x)
        x_extremo = x[x['es_extremo']].reset_index(drop=True).drop(columns=['es_extremo'])
        x_no_extremo = x[~x['es_extremo']].reset_index(drop=True).drop(columns=['es_extremo'])

        temp_scaled = scaler.transform(x_no_extremo[MODEL_FEATURES])
        df_temp_scaled = pd.DataFrame(temp_scaled, columns=SCALED_MODEL_FEATURES)

        x_no_extremo = pd.concat([x_no_extremo.reset_index(drop=True), df_temp_scaled], axis=1)
        
        return x_no_extremo, x_extremo
    
    X_train, X_test = train_test_split(df, test_size=TEST_SIZE, random_state=RANDOM_SEED)

    threshold_energia = X_train["wave_energy"].quantile(EXTREME_THRESHOLD)
    threshold_potencia = X_train["wave_power_kW_m"].quantile(EXTREME_THRESHOLD)

    scaler = StandardScaler()
    scaler.fit(
        X_train.loc[~es_extremo(X_train), MODEL_FEATURES]
    )

    X_train_no_extremo, X_train_extremo = procesar(X_train)
    X_test_no_extremo, X_test_extremo = procesar(X_test)

    gmm = GaussianMixture(
        n_components=N_CLUSTERS,
        covariance_type="full",
        random_state=RANDOM_SEED
    )

    gmm.fit(X_train_no_extremo[SCALED_MODEL_FEATURES])

    X_train_no_extremo["gmm_cluster"] = gmm.predict(X_train_no_extremo[SCALED_MODEL_FEATURES])
    X_train_no_extremo["gmm_cluster_probability"] = gmm.predict_proba(X_train_no_extremo[SCALED_MODEL_FEATURES]).max(axis=1)

    cluster_summary = (
        X_train_no_extremo.groupby("gmm_cluster")[EXTREME_FEATURES]
        .mean()
        .sort_values(EXTREME_FEATURES)
    )
    cluster_order = {
        old_cluster: f'{new_cluster + 1}'
        for new_cluster, old_cluster in enumerate(cluster_summary.index)
    }

    X_test_no_extremo["gmm_cluster"] = gmm.predict(X_test_no_extremo[SCALED_MODEL_FEATURES])
    X_test_no_extremo["gmm_cluster_probability"] = gmm.predict_proba(X_test_no_extremo[SCALED_MODEL_FEATURES]).max(axis=1)

    q_5 = X_test_no_extremo.groupby('gmm_cluster')['gmm_cluster_probability'].quantile(0.05)
    q_10 = X_test_no_extremo.groupby('gmm_cluster')['gmm_cluster_probability'].quantile(0.10)
    q_15 = X_test_no_extremo.groupby('gmm_cluster')['gmm_cluster_probability'].quantile(0.15)
    if not (
        all(X_test_no_extremo.groupby('gmm_cluster')['gmm_cluster_probability'].quantile(0.05)>=0.7) and
        all(X_test_no_extremo.groupby('gmm_cluster')['gmm_cluster_probability'].quantile(0.10)>=0.8) and
        all(X_test_no_extremo.groupby('gmm_cluster')['gmm_cluster_probability'].quantile(0.15)>=0.9) 
    ):
        print(f'El modelo GMM no cumple con los criterios')
        return None, None
    else:
        print(f'El modelo GMM cumple con los criterios')

    print(f'Quantiles: 0.05: {q_5}, 0.10: {q_10}, 0.15: {q_15}')

    X_test_no_extremo = X_test_no_extremo.assign(gmm_cluster=lambda df: df["gmm_cluster"].map(cluster_order))
    X_test_extremo = X_test_extremo.assign(gmm_cluster='7')

    X_train_no_extremo = X_train_no_extremo.assign(gmm_cluster=lambda df: df["gmm_cluster"].map(cluster_order))
    X_train_extremo = X_train_extremo.assign(gmm_cluster='7')

    df_train = pd.concat(
        [
            X_train_no_extremo[['coast_name', 'datetime'] + MODEL_FEATURES + ['gmm_cluster']],
            X_train_extremo[['coast_name', 'datetime'] + MODEL_FEATURES + ['gmm_cluster']]
        ],
        ignore_index=True
    )

    df_test = pd.concat(
        [
            X_test_no_extremo[['coast_name', 'datetime'] + MODEL_FEATURES + ['gmm_cluster']],
            X_test_extremo[['coast_name', 'datetime'] + MODEL_FEATURES + ['gmm_cluster']]
        ],
        ignore_index=True
    )

    return df_train, df_test

def entrenar_rf(df_train, df_test):
    X_train = df_train[MODEL_FEATURES]
    y_train = df_train['gmm_cluster'].astype(int)

    X_test = df_test[MODEL_FEATURES]
    y_test = df_test['gmm_cluster'].astype(int)

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

    acurracy = accuracy_score(y_test, y_pred)
    balanced_acurracy = balanced_accuracy_score(y_test, y_pred)
    
    if not(
        acurracy >= 0.7 and 
        balanced_acurracy >= 0.5
    ):
        print(f'El modelo Random Forest no cumple con los criterios')
        return None
    else:
        print(f'El modelo Random Forest cumple con los criterios')
    
    print(f'Accuracy: {acurracy}, Balanced Accuracy: {balanced_acurracy}')

    return rf_classifier

def main():
    # Obtener datos
    data = (
        spark.sql(
            f"""
                SELECT coast_name, datetime, {', '.join(set(MODEL_FEATURES + EXTREME_FEATURES))},
                CONCAT(coast_name, '_', DATE_FORMAT(datetime, 'yyyyMM')) AS coast_year_month
                FROM cor_{ambiente}.silver.swell_metrics
                WHERE YEAR(datetime) % {EVERY_N_YEARS} = 0
            """
        )
    )
    coast_year_month_dict = {row.coast_year_month: PORCENTAJE_SAMPLE_DATA_MONTH for row in data.select('coast_year_month').distinct().collect()}
    df = (
        data
        .sampleBy('coast_year_month', fractions=coast_year_month_dict, seed=RANDOM_SEED)
        .drop('coast_year_month')
    ).toPandas()

    # Clasificar con GMM
    df_train, df_test = clasificar_gmm(df)
    if df_train is None or df_test is None:
        print('No se pudo clasificar con GMM, no se entrena el modelo Random Forest')
        return
    # Entrenar Random Forest
    rf_classifier = entrenar_rf(df_train, df_test)
    if rf_classifier is None:
        print('No se pudo entrenar el modelo Random Forest')
        return
    # Guardar modelo
    joblib.dump(rf_classifier, model_path)

main()