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
    rf_model_path = f'/Volumes/cor_{ambiente}/ml/models/wave_classifier/wave_classifier.pkl'
else:
    base_path = Path.cwd().parent
    rf_model_path = f'{base_path}/wave_classifier/wave_classifier.pkl'

modelos_disponibles = []

if os.path.exists(rf_model_path):
    modelos_disponibles.append(True)
else:
    modelos_disponibles.append(False)

dbutils.jobs.taskValues.set(
    key="todos_los_modelos_existen",
    value=str(all(modelos_disponibles)).lower()
)