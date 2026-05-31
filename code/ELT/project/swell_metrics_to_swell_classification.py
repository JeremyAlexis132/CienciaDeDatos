import os
import argparse
from pathlib import Path
import joblib
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import pandas_udf

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente", required=True)
args = parser.parse_args()

ambiente = args.ambiente
print(f"Ambiente: {ambiente}")

MODEL_FEATURES = [
    'wind_speed_ms', 
    'wave_energy', 'wave_height_m', 'wave_period_s', 'wave_power_kW_m'
]

sea_state_names = {
    1: "Mar calmado",
    2: "Mar suave",
    3: "Mar dinámico",
    4: "Mar agitado",
    5: "Mar fuerte",
    6: "Mar peligroso",
    7: "Mar extremo"
}

if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
    model_path = f'/Volumes/cor_{ambiente}/ml/models/wave_classifier/wave_classifier.pkl'
else:
    base_path = Path.cwd().parent
    model_path = f'{base_path}/wave_classifier/wave_classifier{{}}.pkl'

_models_cache = None

def get_model():
    global _models_cache
    if _models_cache is None:
        _models_cache = joblib.load(model_path)
    return _models_cache

@pandas_udf(StringType())
def predict_wave_classification(
    wind_speed_ms: pd.Series,
    wave_energy: pd.Series,
    wave_height_m: pd.Series,
    wave_period_s: pd.Series,
    wave_power_kW_m: pd.Series,
) -> pd.Series:
    result = pd.Series(
        ["Categoría desconocida"] * len(wind_speed_ms),
        index=wind_speed_ms.index,
    )

    pdf = pd.DataFrame({
        "wind_speed_ms": wind_speed_ms,
        "wave_energy": wave_energy,
        "wave_height_m": wave_height_m,
        "wave_period_s": wave_period_s,
        "wave_power_kW_m": wave_power_kW_m,
    })

    try:
        X = pdf[MODEL_FEATURES]

        # Evita fallos por nulos en features.
        valid_mask = X.notnull().all(axis=1)

        if valid_mask.any():
            X_valid = X.loc[valid_mask]
            model = get_model()
            predictions = model.predict(X_valid)

            result.loc[X_valid.index] = [
                sea_state_names.get(int(pred), "Categoría desconocida")
                for pred in predictions
            ]

    except Exception as e:
        print(f"Error clasificando wave_classification: {e}")

    return result

df_silver_base = (
    spark.readStream
        .format("delta")
        .table(f"cor_{ambiente}.silver.swell_metrics")
)

df_silver_classified = (
    df_silver_base
        .withColumn(
            "wave_classification",
            predict_wave_classification(
                F.col("wind_speed_ms"),
                F.col("wave_energy"),
                F.col("wave_height_m"),
                F.col("wave_period_s"),
                F.col("wave_power_kW_m"),
            ),
        )
        .withColumn(
            "classification_timestamp",
            F.current_timestamp(),
        )
)

df_silver_classified_clean = (
    df_silver_classified
    .select(
        "id",
        "coast_name",
        "datetime",
        "year",
        "wave_classification",
        "classification_timestamp",
    )
)

query_silver_classified = (
    df_silver_classified_clean.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option(
            "checkpointLocation",
            f"/Volumes/cor_{ambiente}/silver/data/checkpoints/swell_classification",
        )
        .partitionBy("coast_name", "year")
        .toTable(f"cor_{ambiente}.silver.swell_classification")
)