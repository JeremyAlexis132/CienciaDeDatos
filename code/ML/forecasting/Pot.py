import pandas as pd
import numpy as np
from pyextremes import EVA
from joblib import Parallel, delayed
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

# Load the data
df = spark.read.table("cor_project.silver.swell_metrics")
df = df.toPandas()

# ELIMINAR COLUMNAS INSERVIBLES
df = df.drop(columns = ['id','source_file','ingestion_timestamp','transformation_timestamp', 'wind_cos_direction', 'wind_sin_direction', 'wave_cos_direction', 'wave_sin_direction'])

df['datetime'] = pd.to_datetime(df['datetime'])
df = df.set_index('datetime')
df = df.sort_index()

RETURN_PERIODS = [50, 100]
BOOTSTRAP_ITERATIONS = 1000
R_DECLUSTERING = "72h"
THRESHOLD_QUANTILE = 0.95

def run_pot_analysis(ts: pd.Series, coast_name: str):
    """
    Ejecuta el análisis POT
    """
    try:
        threshold = ts.quantile(THRESHOLD_QUANTILE)
        model = EVA(ts)
        
        # Se obtienen los extremos
        model.get_extremes(method="POT", threshold=threshold, r=R_DECLUSTERING)
        
        # Se ajusta el modelo
        model.fit_model(distribution="genpareto")
        
        # Resultados
        results = {"coast": coast_name}
        for rp in RETURN_PERIODS:
            rl, ci_lower, ci_upper = model.get_return_value(
                return_period=rp,
                return_period_size="365.2425D",
                alpha=0.95,
                n_samples=BOOTSTRAP_ITERATIONS
            )
            results[f"RL_{rp}"] = rl
            results[f"CI_lower_{rp}"] = ci_lower
            results[f"CI_upper_{rp}"] = ci_upper
            
        return results
    except Exception as e:
        print(f"Error en {coast_name}: {e}")
        return {"coast": coast_name, "error": str(e)}
    
df_clean = df[['coast_name', 'wave_power_kW_m']].copy()

# Listado de costas
coasts = df_clean['coast_name'].unique()

# Ejecución en paralelo usando todos los núcleos disponibles (n_jobs=-1)
results_list = Parallel(n_jobs=-1)(
    delayed(run_pot_analysis)(
        df_clean[df_clean['coast_name'] == coast]['wave_power_kW_m'].sort_index(), 
        coast
    ) 
    for coast in coasts
)


results_df = pd.DataFrame(results_list)

profiles = []

for coast in df['coast_name'].unique(): #Itera en el dataframe la columna 'coast_name' solo por cada costa
	df_costa = df[df['coast_name'] == coast] # quedarse solo con las filas de la costa actual
	
	# threshold percentile 95
	threshold = df_costa['wave_power_kW_m'].quantile(0.95) # las filas que superen el percentil 95 de 'wave_power_kW_m'.
	
	# Filtrar filas extremas
	extremes = df_costa[df_costa['wave_power_kW_m'] >= threshold][['coast_name', 'wave_height_m', 'wave_direction_deg', 'wave_period_s','wave_power_kW_m']]
 
	profiles.append(extremes) 
	
df_extremes = pd.concat(profiles)

# Para cada costa y cada periodo de retorno
for coast in results_df['coast']:
    # Usar la  mediana del periodo de eventos extremos de esa costa
    T_mediana = df_extremes[
        df_extremes['coast_name'] == coast
    ]['wave_period_s'].median()
    
    # Despejar H para RL50 y RL100
    for rp in [50, 100]:
        RL = results_df[results_df['coast'] == coast][f'RL_{rp}'].values[0]
        
        H_estimada = np.sqrt(RL / (0.49 * T_mediana))
        
        
# 1. Definir los Bins de Potencia
bins = [0, 50, 100, 200, 500, np.inf]
labels = ['bajo', 'medio', 'alto', 'muy_alto', 'extremo']

# 2.Calcula la mediana del periodo para cada "bin" de potencia en los datos extremos
df_extremes['power_bin'] = pd.cut(df_extremes['wave_power_kW_m'], bins=bins, labels=labels)
period_map = df_extremes.groupby('power_bin')['wave_period_s'].median().to_dict()

# 3. Función para asignar periodo según la potencia estimada (RL)
def get_period_for_power(power_value):
    # Identificar a qué bin pertenece la potencia
    bin_name = pd.cut([power_value], bins=bins, labels=labels)[0]
    return period_map.get(bin_name, df_extremes['wave_period_s'].median()) # Fallback a mediana global

# 4. Cálculo de Rangos de Altura (H) usando RL_lower, RL_mean, RL_upper
for rp in [50, 100]:
    for bound in ['lower', 'mean', 'upper']:
        if bound == 'mean':
            P = results_df[f'RL_{rp}']
        else:
            P = results_df[f'CI_{bound}_{rp}']
            
        #Periodo correlacionado la potencia
        T_asociado = P.apply(get_period_for_power)
        
        # Calcular H
        H_estimada = np.sqrt(P / (0.49 * T_asociado))
        
        results_df[f'H_{rp}_{bound}'] = H_estimada

results_df.columns =['Coast','Power_50_Mean', 'Power_50_Lower', 'Power_50_Upper','Power_100_Mean', 'Power_100_Lower', 'Power_100_Upper', 'Height_50_Lower', 'Height_50_Mean', 'Height_50_Upper', 'Height_100_Lower', 'Height_100_Mean', 'Height_100_Upper']

import pandas as pd
import plotly.express as px

# Asegurémonos de que es un DataFrame de Pandas
# df_results = df_results.toPandas() if it's currently a Spark DF

# Definimos los bloques que queremos extraer
# (Variable_Base_Nombre, "Metrica", "Periodo")
# Usaremos las columnas que me diste
data_blocks = [
    ("Power_50", "Power", "50"),
    ("Power_100", "Power", "100"),
    ("Height_50", "Height", "50"),
    ("Height_100", "Height", "100")
]

tidy_list = []

for base_name, metric, period in data_blocks:
    # Seleccionamos las columnas correspondientes a este bloque
    subset = results_df[['Coast', f'{base_name}_Mean', f'{base_name}_Lower', f'{base_name}_Upper']].copy()
    
    # Renombramos para estandarizar
    subset.columns = ['Coast', 'Mean', 'Lower', 'Upper']
    
    # Agregamos etiquetas descriptivas
    subset['Metric'] = metric
    subset['Period'] = period
    
    tidy_list.append(subset)

# Unimos todo en un solo dataframe largo
df_tidy = pd.concat(tidy_list, ignore_index=True)

df_tidy['err_plus'] = df_tidy['Upper'] - df_tidy['Mean']
df_tidy['err_minus'] = df_tidy['Mean'] - df_tidy['Lower']

df_tidy.columns = ['coast_name', 'mean', 'lower', 'upper', 'metric', 'period', 'err_plus', 'err_minus']

# convertir dataframe a spark dataframe
df_tidy = spark.createDataFrame(df_tidy)

# Guardar tabla en gold
(
    df_tidy
    .write
    .format("delta") 
    .mode("append")  
    .option("mergeSchema", "true") 
    .saveAsTable(f"cor_{ambiente}.gold.forecast")
)





