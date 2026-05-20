import os
from pathlib import Path
import joblib
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

MODEL_FEATURES = [
    'wind_speed_ms', 'wave_energy', 
    'wave_period_s', 'wave_steepness'
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
    model_path = f'/Volumes/cor_{ambiente}/ml/models/wave_clasificator/wave_clasificator_{{}}.pkl'
else:
    base_path = Path.cwd().parent
    model_path = f'{base_path}/wave_clasificator/wave_clasificator_{{}}.pkl'

def predecir_categoria_texto(row):
    valores = [[row[c] for c in MODEL_FEATURES]]

    pred_num = model.predict(valores)[0]

    return sea_state_names.get(int(pred_num), "Categoría desconocida")

predecir_udf = F.udf(predecir_categoria_texto, StringType())

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

    df = (
        spark.sql(
            f"""
                SELECT id, coast_name, year,
                {', '.join(MODEL_FEATURES)}
                FROM cor_{ambiente}.silver.swell_metrics
                WHERE coast_name = '{coast}'
                AND wave_classification IS NULL
            """
        )
    )

    model = joblib.load(model_path.format(coast))

    df_resultado = (
        df
        .withColumn(
            "wave_classification",
            predecir_udf(F.struct(*MODEL_FEATURES))
        )
    )
    df_updates = (
        df_resultado
        .select(
            "id",
            "coast_name",
            "year",
            "wave_classification"
        )
    )

    df_updates.createOrReplaceTempView("updates")

    spark.sql(
        f"""
            MERGE INTO cor_{ambiente}.silver.swell_metrics AS target
            USING updates AS source
            ON target.id = source.id
            AND target.coast_name = source.coast_name
            AND target.year = source.year
            WHEN MATCHED AND (
                target.wave_classification IS NULL
                OR target.wave_classification <> source.wave_classification
            )
            THEN UPDATE SET 
                target.wave_classification = source.wave_classification,
                target.classification_timestamp = current_timestamp()
        """
    )