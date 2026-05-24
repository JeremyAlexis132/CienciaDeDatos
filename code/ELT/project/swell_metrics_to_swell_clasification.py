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

if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
    model_path = f'/Volumes/cor_{ambiente}/ml/models/wave_clasificator/wave_clasificator_{{}}.pkl'
else:
    base_path = Path.cwd().parent
    model_path = f'{base_path}/wave_clasificator/wave_clasificator_{{}}.pkl'

MODEL_FEATURES = [
    "wind_speed_ms",
    "wave_energy",
    "wave_period_s",
    "wave_steepness"
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

_models_cache = {}


def get_model(coast_name: str):
    if coast_name not in _models_cache:
        _models_cache[coast_name] = joblib.load(model_path.format(coast_name))
    return _models_cache[coast_name]

@pandas_udf(StringType())
def predict_wave_classification(
    coast_name: pd.Series,
    wind_speed_ms: pd.Series,
    wave_energy: pd.Series,
    wave_period_s: pd.Series,
    wave_steepness: pd.Series
) -> pd.Series:
    result = pd.Series(
        ["Categoría desconocida"] * len(coast_name),
        index=coast_name.index
    )

    pdf = pd.DataFrame({
        "coast_name": coast_name,
        "wind_speed_ms": wind_speed_ms,
        "wave_energy": wave_energy,
        "wave_period_s": wave_period_s,
        "wave_steepness": wave_steepness
    })

    for coast in pdf["coast_name"].dropna().unique():
        if coast == "":
            continue

        mask = pdf["coast_name"] == coast

        try:
            model = get_model(coast)

            X = pdf.loc[mask, MODEL_FEATURES]

            # Evita fallos por nulos en features.
            valid_mask = X.notnull().all(axis=1)

            if valid_mask.any():
                X_valid = X.loc[valid_mask]
                predictions = model.predict(X_valid)

                result.loc[X_valid.index] = [
                    sea_state_names.get(int(pred), "Categoría desconocida")
                    for pred in predictions
                ]

        except Exception as e:
            print(f"Error clasificando coast_name={coast}: {e}")
            result.loc[mask] = "Categoría desconocida"

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
                F.col("coast_name"),
                F.col("wind_speed_ms"),
                F.col("wave_energy"),
                F.col("wave_period_s"),
                F.col("wave_steepness")
            )
        )
        .withColumn(
            "classification_timestamp", 
            F.current_timestamp()
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
        "classification_timestamp"
    )
)

query_silver_classified = (
    df_silver_classified_clean.writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option(
            "checkpointLocation",
            f"/Volumes/cor_{ambiente}/silver/data/checkpoints/swell_clasification"
        )
        .partitionBy("coast_name", "year")
        .toTable(f"cor_{ambiente}.silver.swell_clasification")
)