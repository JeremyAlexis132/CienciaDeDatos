import os
from pathlib import Path
import argparse

# Argumentos
parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()
ambiente = args.ambiente
print(ambiente)

if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
    scaler_path = f'/Volumes/cor_{ambiente}/ml/models/scaler/scaler_{{}}.pkl'
    model_path = f'/Volumes/cor_{ambiente}/ml/models/wave_clasificator/wave_clasificator_{{}}.pkl'
else:
    base_path = Path.cwd().parent
    scaler_path = f'{base_path}/scaler/scaler_{{}}.pkl'
    model_path = f'{base_path}/wave_clasificator/wave_clasificator_{{}}.pkl'

coast_names = spark.sql(
    f"""
    SELECT DISTINCT coast_name
    FROM cor_{ambiente}.silver.swell_metrics
    """
).toPandas()['coast_name'].tolist()

modelos_disponibles = []
modelos_faltantes = ''
for coast in coast_names:
    scaler_file = scaler_path.format(coast)
    model_file = model_path.format(coast)
    if os.path.exists(scaler_file) and os.path.exists(model_file):
        modelos_disponibles.append(True)
    else:
        modelos_faltantes += f"{coast},"
        modelos_disponibles.append(False)

modelos_faltantes = modelos_faltantes.rstrip(",")

dbutils.jobs.taskValues.set(
    key="todos_los_modelos_existen",
    value=str(all(modelos_disponibles)).lower()
)

dbutils.jobs.taskValues.set(
    key="modelos_faltantes",
    value=str(modelos_faltantes)
)