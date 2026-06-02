# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard Costero v5 — Atlas Costero alimentado por la capa *gold* de `cor_project`
# MAGIC
# MAGIC Genera un HTML autocontenido con un mapa de México de marcadores clicables. Al hacer
# MAGIC clic en una costa se abre un modal con **cuatro pestañas**:
# MAGIC
# MAGIC - **Vista** — KPIs y gráficas de oleaje/viento · fuente `wave_daily_summary`
# MAGIC - **Estado del mar** — composición mensual de las 7 clases · fuente `wave_monthly_classification`
# MAGIC - **Diseño** — altura de ola a 50 y 100 años con IC 95% · fuente `significant_wave_height_forecast`
# MAGIC - **Descripción** — ficha estática por costa (estado, texto, imagen)
# MAGIC
# MAGIC ### Cambios frente al v4
# MAGIC - Tres tablas *gold* de `cor_project` en lugar de una tabla *silver*.
# MAGIC - Las rosas direccionales del v4 **no** se reconstruyen: la tabla de vectores
# MAGIC   (`wave_monthly_metric_vector`) no existe en `cor_project`. Si se promueve desde
# MAGIC   `cor_dev`, se puede reañadir esa pestaña.
# MAGIC - El ajuste Gumbel del v4 se sustituye por la pestaña **Diseño**, que usa las
# MAGIC   predicciones reales de retorno a 50/100 años.
# MAGIC
# MAGIC > Antes de correr asegúrate de tener `prototipo_dashboard_costero.html` en tu workspace
# MAGIC > (de ahí se extrae la imagen base64 del mapa).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Parámetros

# COMMAND ----------

# --- Entorno -----------------------------------------------------------------
CATALOG     = "cor_project"     # producción. Cambia a "cor_dev" solo para probar.
SCHEMA_GOLD = "gold"

TBL_DAILY    = f"{CATALOG}.{SCHEMA_GOLD}.wave_daily_summary"
TBL_CLASS    = f"{CATALOG}.{SCHEMA_GOLD}.wave_monthly_classification"
TBL_FORECAST = f"{CATALOG}.{SCHEMA_GOLD}.significant_wave_height_forecast"

SOURCE_LABEL = f"{CATALOG}.{SCHEMA_GOLD} · daily_summary + monthly_classification + forecast"

PROTOTYPE_HTML_PATH = "/Workspace/Users/santiagogalap@gmail.com/CienciaDeDatos/src/dashboard/dashboard_costero_v4.html"
HTML_OUTPUT_PATH    = "/Workspace/Users/santiagogalap@gmail.com/CienciaDeDatos/src/dashboardHTML/dashboard_costero_v5.html"

# --- KPI de viento -----------------------------------------------------------
# wave_daily_summary NO tiene columna de viento promedio: solo max/min por día.
#   "max" -> avg(max_wave_wind_speed_ms): viento asociado a la ola más alta del día.
#   "mid" -> avg((max + min) / 2):       viento típico del día (punto medio).
# El KPI se etiqueta según el modo, sin afirmar que es una media real.
WIND_MODE = "max"   # "max" | "mid"

# --- Clases del estado del mar, en orden de calmo a extremo ------------------
# Confirmadas con SELECT DISTINCT wave_classification (7 categorías reales;
# el comentario de la tabla decía Calm/Moderate/Rough pero está desactualizado).
SEA_STATE_ORDER = [
    "Mar calmado",
    "Mar suave",
    "Mar dinámico",
    "Mar agitado",
    "Mar fuerte",
    "Mar peligroso",
    "Mar extremo",
]
SEA_STATE_COLORS = {
    "Mar calmado":  "#4B7158",
    "Mar suave":    "#7C9A66",
    "Mar dinámico": "#C9B45E",
    "Mar agitado":  "#D89A4E",
    "Mar fuerte":   "#D85D3F",
    "Mar peligroso":"#A8392A",
    "Mar extremo":  "#6E1F17",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Metadata y descripción de cada costa
# MAGIC
# MAGIC Las llaves `coast_name` deben coincidir EXACTAMENTE con los valores de la tabla.
# MAGIC `x`/`y` son las posiciones del marcador sobre el mapa (viewBox 1000×643).
# MAGIC `image` admite una ruta o un data-URI base64; con `None` se muestra un placeholder.

# COMMAND ----------

COAST_META = {
    "Matamoros": {
        "id": "matamoros", "x": 640, "y": 245, "region": "Golfo de México",
        "state": "Tamaulipas", "num": "01", "color": "coral", "image": None,
        "description": [
            "Matamoros se asienta en Tamaulipas, en la desembocadura del río Bravo sobre el Golfo de México, en una costa baja y arenosa.",
            "Su economía combina industria, comercio fronterizo y pesca. La franja costera queda expuesta a los frentes fríos del invierno ('nortes') y al paso de ciclones tropicales en temporada.",
        ],
    },
    "Tuxpan": {
        "id": "tuxpan", "x": 652, "y": 418, "region": "Golfo de México",
        "state": "Veracruz", "num": "02", "color": "coral", "image": None,
        "description": [
            "Tuxpan es una ciudad portuaria de Veracruz situada sobre el Golfo de México, junto a la desembocadura del río del mismo nombre.",
            "Su actividad gira en torno al puerto, la industria petrolera, la pesca y el turismo de playa. Está expuesta a los 'nortes' y, ocasionalmente, a huracanes.",
        ],
    },
    "Sabancuy": {
        "id": "sabancuy", "x": 835, "y": 498, "region": "Golfo de México",
        "state": "Campeche", "num": "03", "color": "coral", "image": None,
        "description": [
            "Sabancuy es una localidad costera de Campeche ubicada junto a un estero, en el entorno protegido de la Laguna de Términos.",
            "Su economía descansa en la pesca y el ecoturismo. El relativo abrigo de la laguna modera el oleaje buena parte del año, aunque la zona es sensible a ciclones.",
        ],
    },
    "Playa del Carmen": {
        "id": "carmen", "x": 935, "y": 470, "region": "Mar Caribe",
        "state": "Quintana Roo", "num": "04", "color": "coral", "image": None,
        "description": [
            "Playa del Carmen es un destino turístico de la Riviera Maya, en Quintana Roo, sobre el Mar Caribe y frente a la isla de Cozumel.",
            "El turismo es el motor económico. El arrecife y la isla atenúan parte del oleaje, pero la zona queda expuesta al oleaje del Caribe y a huracanes en temporada.",
        ],
    },
    "Salina Cruz": {
        "id": "salinacruz", "x": 725, "y": 585, "region": "Pacífico Sur",
        "state": "Oaxaca", "num": "05", "color": "moss", "image": None,
        "description": [
            "Salina Cruz es un puerto industrial de Oaxaca en el Golfo de Tehuantepec, sobre el Pacífico sur.",
            "Concentra refinación, actividad portuaria y pesca. Es célebre por los vientos 'Tehuano', rachas intensas que cruzan el istmo y generan un régimen de viento y oleaje muy energético.",
        ],
    },
    "Puerto Vallarta": {
        "id": "vallarta", "x": 412, "y": 430, "region": "Pacífico Central",
        "state": "Jalisco", "num": "06", "color": "moss", "image": None,
        "description": [
            "Puerto Vallarta se ubica en Jalisco, sobre la Bahía de Banderas, en el Pacífico central.",
            "El turismo es el motor económico. El abrigo relativo de la bahía modera el oleaje buena parte del año, aunque la zona queda expuesta a ciclones del Pacífico durante la temporada.",
        ],
    },
    "Punta Colonet": {
        "id": "colonet", "x": 113, "y": 78, "region": "Pacífico Norte",
        "state": "Baja California", "num": "07", "color": "moss", "image": None,
        "description": [
            "Punta Colonet se encuentra en Baja California, sobre el Pacífico norte, en una costa abierta de acantilados y playas batidas por el océano abierto.",
            "La economía local se asocia a la agricultura y la pesca, y la zona ha figurado en proyectos portuarios. Su exposición directa a los oleajes de fondo ('swell') del noroeste produce un régimen de oleaje energético.",
        ],
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Extraer la imagen del mapa del prototipo

# COMMAND ----------

import os, re, json

with open(PROTOTYPE_HTML_PATH, "r", encoding="utf-8") as f:
    proto_html = f.read()

m = re.search(r'data:image/png;base64,([A-Za-z0-9+/=]+)', proto_html)
if not m:
    raise RuntimeError(
        "No encontré la imagen base64 del mapa dentro del prototipo. "
        "Verifica que PROTOTYPE_HTML_PATH apunta al archivo correcto."
    )
MAP_IMAGE_URI = m.group(0)
print(f"Imagen del mapa extraída ({len(m.group(1)) / 1024:.1f} KB de base64).")

# COMMAND ----------

import base64

IMG_DIR = "/Workspace/Users/santiagogalap@gmail.com/CienciaDeDatos/src/images"

def img_to_data_uri(path):
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    mime = "jpeg" if ext in ("jpg", "jpeg") else ext
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:image/{mime};base64,{b64}"

for coast, meta in COAST_META.items():
    ruta = f"{IMG_DIR}/{coast}.jpg"
    try:
        meta["image"] = img_to_data_uri(ruta)
        print(f"[ok]    {coast}")
    except FileNotFoundError:
        meta["image"] = None  # se queda con el placeholder
        print(f"[falta] {coast}  ->  {ruta}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Carga de las tres tablas gold

# COMMAND ----------

from pyspark.sql import functions as F

# wave_daily_summary: una fila por costa por día. Derivamos year/month de `date`.
df_daily = (
    spark.table(TBL_DAILY)
    .withColumn("year",  F.year(F.col("date")))
    .withColumn("month", F.month(F.col("date")))
)

# wave_monthly_classification: porcentaje de días del mes por clase de estado del mar.
df_class = spark.table(TBL_CLASS)

# significant_wave_height_forecast: alturas de diseño a 50/100 años (puede venir vacía).
df_forecast = spark.table(TBL_FORECAST)

print("daily   :", df_daily.count(), "filas")
print("class   :", df_class.count(), "filas")
print("forecast:", df_forecast.count(), "filas",
      "(vacía -> la pestaña Diseño mostrará placeholder)" if df_forecast.count() == 0 else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Agregados
# MAGIC
# MAGIC **Nota metodológica:** `wave_daily_summary` ya está agregada a nivel diario, así que
# MAGIC los promedios mensuales/globales son *promedios de promedios diarios*. Es válido para
# MAGIC un dashboard; queda documentado aquí.

# COMMAND ----------

# Expresión de viento según WIND_MODE (no existe avg de viento en la tabla).
if WIND_MODE == "mid":
    wind_col = (F.col("max_wave_wind_speed_ms") + F.col("min_wave_wind_speed_ms")) / 2.0
    WIND_KPI_LABEL = "Viento (punto medio)"
else:  # "max"
    wind_col = F.col("max_wave_wind_speed_ms")
    WIND_KPI_LABEL = "Viento (oleaje máx.)"

df_daily = df_daily.withColumn("_wind", wind_col)

# Potencia del oleaje (flujo de energía por metro de cresta), proxy diario.
# P[kW/m] ≈ 0.49 · Hs² · Te  (aguas profundas). Hs = altura promedio diaria;
# Te se aproxima con el punto medio del periodo diario (la tabla solo trae
# max/min de periodo, no promedio). Queda documentado como proxy.
_period_mid = (F.col("max_wave_period_s") + F.col("min_wave_period_s")) / 2.0
df_daily = df_daily.withColumn(
    "_power", 0.49 * F.col("avg_wave_height_m") * F.col("avg_wave_height_m") * _period_mid
)

# --- KPIs por costa ----------------------------------------------------------
kpis = (
    df_daily.groupBy("coast_name")
    .agg(
        F.avg("avg_wave_height_m").alias("mean_hs"),   # oleaje promedio
        F.max("max_wave_height_m").alias("max_hs"),    # oleaje máximo histórico
        F.avg("_wind").alias("mean_wind"),             # viento (según WIND_MODE)
        F.min("year").alias("year_min"),
        F.max("year").alias("year_max"),
        F.count("*").alias("n_records"),
    )
    .toPandas()
)

# --- Estacionalidad mensual --------------------------------------------------
monthly = (
    df_daily.groupBy("coast_name", "month")
    .agg(
        F.avg("avg_wave_height_m").alias("mean_hs"),
        F.max("max_wave_height_m").alias("max_hs"),
        F.avg("_wind").alias("mean_wind"),
        F.avg("_power").alias("mean_power"),
    )
    .orderBy("coast_name", "month")
    .toPandas()
)

# --- Máximo anual (con periodo y viento para las etiquetas de la gráfica) -----
annual = (
    df_daily.groupBy("coast_name", "year")
    .agg(
        F.max("max_wave_height_m").alias("max_hs"),
        F.max("max_wave_period_s").alias("max_period"),
        F.max("max_wave_wind_speed_ms").alias("max_wind"),
    )
    .orderBy("coast_name", "year")
    .toPandas()
)

# --- Mensual desglosado por año ----------------------------------------------
# Insumo del filtro deslizable de la pestaña Vista: el navegador recombina estos
# valores dentro del rango de años elegido. Guardamos el conteo de días `n` para
# poder hacer un promedio ponderado exacto al combinar varios años.
monthly_yr = (
    df_daily.groupBy("coast_name", "year", "month")
    .agg(
        F.avg("avg_wave_height_m").alias("mean_hs"),
        F.max("max_wave_height_m").alias("max_hs"),
        F.avg("_wind").alias("mean_wind"),
        F.avg("_power").alias("mean_power"),
        F.count("*").alias("n"),
    )
    .orderBy("coast_name", "year", "month")
    .toPandas()
)

# --- Estado del mar: climatología mensual por clase --------------------------
# percentage por (costa, año, mes) suma ~100 entre clases. Promediamos sobre los
# años para obtener una composición típica por mes que siga sumando ~100.
seastate = (
    df_class.groupBy("coast_name", "month", "wave_classification")
    .agg(F.avg("percentage").alias("pct"))
    .toPandas()
)

# --- Forecast: última predicción por costa -----------------------------------
forecast = df_forecast.toPandas()
print("Costas en daily:", sorted(kpis["coast_name"].unique().tolist()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Empaquetar como JSON

# COMMAND ----------

import pandas as pd

# Normalizador de mes: la columna `month` de la clasificación es string y puede
# venir como número ("1".."12"), nombre en español o en inglés. Lo mapeamos a 1-12.
_MONTHS = {
    **{str(i): i for i in range(1, 13)},
    **{f"{i:02d}": i for i in range(1, 13)},
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,"julio":7,
    "agosto":8,"septiembre":9,"setiembre":9,"octubre":10,"noviembre":11,"diciembre":12,
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,
    "august":8,"september":9,"october":10,"november":11,"december":12,
    "jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "ene":1,"abr":4,"ago":8,"dic":12,
}
def norm_month(v):
    if v is None: return None
    return _MONTHS.get(str(v).strip().lower())

def safe(x, decimals=2):
    if x is None: return None
    try:
        v = float(x)
        return None if pd.isna(v) else round(v, decimals)
    except (TypeError, ValueError):
        return None

def build_seastate(coast):
    """Matriz {clase: [12 porcentajes]} en el orden SEA_STATE_ORDER. None si no hay datos.
    Robusto a la escala: si `percentage` viene como proporción (0-1) lo convierte a 0-100."""
    s = seastate[seastate["coast_name"] == coast]
    if s.empty:
        return None
    grid = {cls: [None] * 12 for cls in SEA_STATE_ORDER}
    seen = False
    vmax = 0.0
    for _, r in s.iterrows():
        mi = norm_month(r["month"])
        cls = r["wave_classification"]
        if mi is None or cls not in grid:
            continue
        v = safe(r["pct"], 4)
        grid[cls][mi - 1] = v
        if v is not None and v > vmax:
            vmax = v
        seen = True
    if not seen:
        return None
    # Si el mayor valor es <= 1.5, la columna está en proporción: escalar a 0-100.
    if vmax <= 1.5:
        for cls in grid:
            grid[cls] = [None if v is None else round(v * 100, 1) for v in grid[cls]]
    else:
        for cls in grid:
            grid[cls] = [None if v is None else round(v, 1) for v in grid[cls]]
    return {"months": list(range(1, 13)), "series": grid}

def build_forecast(coast):
    """Última predicción de altura de diseño 50/100 años. None si no hay filas."""
    f = forecast[forecast["coast_name"] == coast]
    if f.empty:
        return None
    if "prediction_timestamp" in f.columns and f["prediction_timestamp"].notna().any():
        f = f.sort_values("prediction_timestamp").iloc[[-1]]
    row = f.iloc[0]
    ts = row.get("prediction_timestamp")
    return {
        "timestamp": None if pd.isna(ts) else str(ts),
        "h50":  {"lower": safe(row["H_50_lower"]),  "mean": safe(row["H_50_mean"]),  "upper": safe(row["H_50_upper"])},
        "h100": {"lower": safe(row["H_100_lower"]), "mean": safe(row["H_100_mean"]), "upper": safe(row["H_100_upper"])},
    }

def build_monthly_by_year(coast):
    """{ '<año>': {mean_hs:[12], max_hs:[12], mean_wind:[12], n:[12]} }. None si no hay datos."""
    s = monthly_yr[monthly_yr["coast_name"] == coast]
    if s.empty:
        return None
    out = {}
    for _, r in s.iterrows():
        y = int(r["year"]); mi = int(r["month"])
        if mi < 1 or mi > 12:
            continue
        slot = out.setdefault(str(y), {
            "mean_hs": [None] * 12, "max_hs": [None] * 12,
            "mean_wind": [None] * 12, "mean_power": [None] * 12, "n": [None] * 12,
        })
        slot["mean_hs"][mi - 1]    = safe(r["mean_hs"], 3)
        slot["max_hs"][mi - 1]     = safe(r["max_hs"], 3)
        slot["mean_wind"][mi - 1]  = safe(r["mean_wind"], 3)
        slot["mean_power"][mi - 1] = safe(r["mean_power"], 2)
        slot["n"][mi - 1]          = int(r["n"]) if not pd.isna(r["n"]) else None
    return out or None

coast_data = {}
for coast, meta in COAST_META.items():
    base = {
        "name": coast, "region": meta["region"], "state": meta["state"],
        "num": meta["num"], "color": meta["color"],
        "x": meta["x"], "y": meta["y"],
        "description": meta["description"], "image": meta["image"],
        "wind_label": WIND_KPI_LABEL,
        "seastate": build_seastate(coast),
        "forecast": build_forecast(coast),
    }

    k_row = kpis[kpis["coast_name"] == coast]
    if k_row.empty:
        base["has_data"] = False
        coast_data[meta["id"]] = base
        print(f"[sin daily] {coast}")
        continue

    k = k_row.iloc[0]
    m_df = monthly[monthly["coast_name"] == coast].sort_values("month")
    a_df = annual[annual["coast_name"] == coast].sort_values("year")
    base.update({
        "has_data": True,
        "kpi_mean":  safe(k["mean_hs"], 2),
        "kpi_max":   safe(k["max_hs"],  2),
        "kpi_wind":  safe(k["mean_wind"], 2),
        "year_min":  int(k["year_min"]),
        "year_max":  int(k["year_max"]),
        "n_records": int(k["n_records"]),
        "monthly": {
            "months":    m_df["month"].astype(int).tolist(),
            "mean_hs":   [safe(v, 3) for v in m_df["mean_hs"]],
            "max_hs":    [safe(v, 3) for v in m_df["max_hs"]],
            "mean_wind": [safe(v, 3) for v in m_df["mean_wind"]],
            "mean_power":[safe(v, 2) for v in m_df["mean_power"]],
        },
        "annual": {
            "years":      a_df["year"].astype(int).tolist(),
            "max_hs":     [safe(v, 2) for v in a_df["max_hs"]],
            "max_period": [safe(v, 1) for v in a_df["max_period"]],
            "max_wind":   [safe(v, 1) for v in a_df["max_wind"]],
        },
        "monthly_by_year": build_monthly_by_year(coast),
    })
    coast_data[meta["id"]] = base
    fc = "forecast OK" if base["forecast"] else "forecast vacío"
    ss = "estado OK" if base["seastate"] else "estado vacío"
    print(f"[ok] {coast:<18} ({meta['num']})  Hs̄={k['mean_hs']:.2f}m  Hs_max={k['max_hs']:.2f}m  ·  {ss} · {fc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Plantilla HTML — mapa + modal con pestañas

# COMMAND ----------

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Monitoreo Costero · Análisis de Oleaje (v5)</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,300..900;1,9..144,300..900&family=Manrope:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
  :root {
    --paper: #F4EDDF;
    --paper-soft: #FAF5E8;
    --ink: #0E2235;
    --ink-soft: #2C435A;
    --ink-faint: rgba(14, 34, 53, 0.45);
    --rule: rgba(14, 34, 53, 0.18);
    --coral: #D85D3F;
    --moss: #4B7158;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; }
  body {
    font-family: 'Manrope', sans-serif;
    background: var(--paper);
    color: var(--ink);
    font-size: 14px; line-height: 1.5; overflow: hidden;
  }
  header {
    height: 56px; padding: 0 28px;
    display: flex; align-items: center; justify-content: space-between;
    border-bottom: 1px solid var(--rule); background: var(--paper);
  }
  header h1 { font-family: 'Fraunces', serif; font-weight: 400; font-size: 17px; letter-spacing: -0.01em; }
  header h1 em { font-style: italic; font-weight: 300; }
  header h1 .version { font-family: 'JetBrains Mono', monospace; font-size: 9px; letter-spacing: 0.12em; color: var(--ink-faint); margin-left: 8px; }
  .header-meta { font-family: 'JetBrains Mono', monospace; font-size: 10px; text-transform: uppercase; letter-spacing: 0.12em; color: var(--ink-faint); display: flex; gap: 24px; }
  .header-meta strong { color: var(--ink); font-weight: 500; }

  .map-wrap { position: relative; width: 100%; height: calc(100vh - 56px); min-height: 540px; overflow: hidden; background: var(--paper); display: flex; align-items: center; justify-content: center; padding: 24px; }
  .map-frame { position: relative; aspect-ratio: 1000 / 643; width: 100%; height: 100%; max-width: 100%; max-height: 100%; }
  .map-img, .map-overlay { position: absolute; inset: 0; width: 100%; height: 100%; display: block; }
  .map-img { object-fit: contain; user-select: none; pointer-events: none; }
  .map-overlay { pointer-events: none; }
  .map-overlay .marker-group { pointer-events: auto; cursor: pointer; }

  .marker-pulse { transform-origin: center; animation: pulse 2.4s ease-out infinite; }
  @keyframes pulse { 0% { r: 8; opacity: 0.55; } 100% { r: 28; opacity: 0; } }
  .marker-core { transition: r 0.25s ease, stroke-width 0.25s ease; }
  .marker-group:hover .marker-core { r: 9; stroke-width: 3; }
  .marker-label { font-family: 'Fraunces', serif; font-size: 14px; font-style: italic; fill: var(--ink); pointer-events: none; }
  .marker-tag { font-family: 'JetBrains Mono', monospace; font-size: 8px; letter-spacing: 0.15em; fill: var(--ink-faint); text-transform: uppercase; pointer-events: none; }

  .modal-backdrop { position: fixed; inset: 0; background: rgba(14, 34, 53, 0.55); display: none; align-items: center; justify-content: center; z-index: 50; opacity: 0; transition: opacity 0.25s ease; }
  .modal-backdrop.open { display: flex; opacity: 1; }
  .modal { width: 94vw; max-width: 1320px; height: 92vh; max-height: 860px; min-height: 560px; background: white; border-radius: 8px; padding: 18px 22px 22px; display: grid; grid-template-rows: auto auto 1fr; gap: 12px; box-shadow: 0 20px 60px rgba(0,0,0,0.3); transform: translateY(20px) scale(0.98); transition: transform 0.3s cubic-bezier(0.16, 1, 0.3, 1); }
  .modal-backdrop.open .modal { transform: translateY(0) scale(1); }

  .modal-header { display: flex; justify-content: space-between; align-items: flex-start; padding-bottom: 12px; border-bottom: 1px solid #e5e5e5; }
  .modal-coast-tag { font-family: 'JetBrains Mono', monospace; font-size: 9px; letter-spacing: 0.18em; text-transform: uppercase; color: var(--ink-faint); margin-bottom: 4px; display: inline-flex; align-items: center; gap: 8px; }
  .modal-coast-tag::before { content: ""; width: 8px; height: 8px; border-radius: 50%; background: var(--coral); }
  .modal-coast-tag.moss::before { background: var(--moss); }
  .modal-title { font-family: 'Fraunces', serif; font-weight: 300; font-size: 28px; line-height: 1.1; letter-spacing: -0.02em; color: var(--ink); }
  .modal-period { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--ink-faint); margin-top: 4px; }
  .modal-close { background: transparent; border: 1px solid #d0d4d8; width: 32px; height: 32px; border-radius: 50%; cursor: pointer; display: grid; place-items: center; color: var(--ink); font-size: 18px; line-height: 1; transition: background 0.2s, color 0.2s, transform 0.2s; }
  .modal-close:hover { background: var(--ink); color: white; transform: rotate(90deg); }

  /* Tabs */
  .tabbar { display: flex; gap: 4px; border-bottom: 1px solid #e5e5e5; }
  .tab { font-family: 'JetBrains Mono', monospace; font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase; color: var(--ink-faint); background: transparent; border: none; border-bottom: 2px solid transparent; padding: 8px 14px; cursor: pointer; transition: color 0.2s, border-color 0.2s; }
  .tab:hover { color: var(--ink-soft); }
  .tab.active { color: var(--ink); border-bottom-color: var(--coral); }

  .tab-pane { display: none; overflow: hidden; }
  .tab-pane.active { display: grid; }

  /* Vista */
  .pane-vista { grid-template-columns: 280px 1fr 1fr; grid-template-rows: auto 1fr 1fr; gap: 10px; }
  .panel { background: #fafafa; border: 1px solid #ececec; border-radius: 6px; padding: 12px 16px 14px; display: flex; flex-direction: column; overflow: hidden; }
  .panel-title { font-size: 11px; font-weight: 600; color: #555; margin-bottom: 8px; }
  .vista-filter { grid-column: 1 / 4; grid-row: 1; padding: 10px 16px 12px; }
  .kpi-col { grid-column: 1; grid-row: 2 / 4; display: flex; flex-direction: column; gap: 10px; }
  .kpi { flex: 1; background: #fafafa; border: 1px solid #ececec; border-radius: 6px; padding: 14px; display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center; }
  .kpi-value { font-family: 'Fraunces', serif; font-weight: 300; font-size: 52px; line-height: 1; color: var(--ink); }
  .kpi-unit { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--ink-faint); margin-top: 2px; }
  .kpi-label { font-size: 11px; color: #777; margin-top: 10px; text-transform: uppercase; letter-spacing: 0.05em; }
  .annual-panel { grid-column: 2 / 4; grid-row: 2; }
  .seasonality-panel { grid-column: 2; grid-row: 3; }
  .power-panel { grid-column: 3; grid-row: 3; }
  .chart { flex: 1; min-height: 0; }

  /* Filtro de años (slider de doble manija) */
  .filter-head { display: flex; align-items: baseline; justify-content: space-between; margin-bottom: 10px; }
  .filter-title { font-size: 11px; font-weight: 600; color: #555; }
  .filter-readout { font-family: 'JetBrains Mono', monospace; font-size: 13px; color: var(--ink); }
  .filter-readout small { color: var(--ink-faint); font-size: 10px; }
  .range-wrap { position: relative; height: 22px; }
  .range-track { position: absolute; top: 9px; left: 0; right: 0; height: 4px; border-radius: 2px; background: #dfdacb; }
  .range-fill  { position: absolute; top: 0; height: 4px; border-radius: 2px; background: var(--coral); }
  .range-wrap input[type="range"] {
    -webkit-appearance: none; appearance: none; position: absolute; top: 0; left: 0;
    width: 100%; height: 22px; margin: 0; background: transparent; pointer-events: none;
  }
  .range-wrap input[type="range"]::-webkit-slider-thumb {
    -webkit-appearance: none; appearance: none; pointer-events: auto; cursor: pointer;
    width: 16px; height: 16px; border-radius: 50%; background: #fff;
    border: 3px solid var(--coral); box-shadow: 0 1px 3px rgba(0,0,0,0.25); margin-top: 0;
  }
  .range-wrap input[type="range"]::-moz-range-thumb {
    pointer-events: auto; cursor: pointer; width: 16px; height: 16px; border-radius: 50%;
    background: #fff; border: 3px solid var(--coral); box-shadow: 0 1px 3px rgba(0,0,0,0.25);
  }
  .range-wrap input[type="range"]::-webkit-slider-runnable-track { background: transparent; }
  .range-wrap input[type="range"]::-moz-range-track { background: transparent; }

  /* Estado del mar */
  .pane-sea { grid-template-rows: 1fr; }
  .pane-sea .panel { grid-column: 1; }

  /* Diseño */
  .pane-design { grid-template-columns: 1fr 1fr; grid-template-rows: auto 1fr; gap: 12px; }
  .design-cards { grid-column: 1 / 3; grid-row: 1; display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .design-card { background: #fafafa; border: 1px solid #ececec; border-radius: 6px; padding: 16px 18px; }
  .design-card .dc-head { font-family: 'JetBrains Mono', monospace; font-size: 10px; letter-spacing: 0.12em; text-transform: uppercase; color: var(--ink-faint); }
  .design-card .dc-mean { font-family: 'Fraunces', serif; font-weight: 300; font-size: 44px; line-height: 1.1; color: var(--ink); }
  .design-card .dc-mean small { font-family: 'JetBrains Mono', monospace; font-size: 13px; color: var(--ink-faint); }
  .design-card .dc-ci { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--ink-soft); margin-top: 4px; }
  .design-chart-panel { grid-column: 1 / 3; grid-row: 2; }

  /* Descripción */
  .pane-info { grid-template-columns: 1fr 1.1fr; gap: 18px; align-content: start; }
  .info-image { background: #f0ece2; border: 1px solid #e5dfd1; border-radius: 6px; width: 100%; height: 100%; min-height: 280px; display: flex; align-items: center; justify-content: center; overflow: hidden; }
  .info-image img { width: 100%; height: 100%; object-fit: cover; }
  .info-image .ph { font-family: 'JetBrains Mono', monospace; font-size: 10px; letter-spacing: 0.12em; text-transform: uppercase; color: var(--ink-faint); text-align: center; padding: 20px; }
  .info-text { display: flex; flex-direction: column; gap: 14px; }
  .info-state { font-family: 'JetBrains Mono', monospace; font-size: 11px; letter-spacing: 0.1em; text-transform: uppercase; color: var(--coral); }
  .info-text.moss .info-state { color: var(--moss); }
  .info-text p { color: var(--ink-soft); font-size: 14px; line-height: 1.65; }

  /* Empty / placeholder */
  .empty-state { display: flex; flex-direction: column; align-items: center; justify-content: center; color: var(--ink-faint); font-family: 'Fraunces', serif; font-style: italic; font-size: 17px; gap: 8px; text-align: center; padding: 30px; min-height: 200px; }
  .empty-state small { font-family: 'JetBrains Mono', monospace; font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase; font-style: normal; }
</style>
</head>
<body>

<header>
  <h1>Monitoreo Costero <em>· Análisis de Oleaje</em><span class="version">V5 · GOLD</span></h1>
  <div class="header-meta">
    <span>Costas · <strong id="coast-count">—</strong></span>
    <span>Fuente · <strong>__SOURCE__</strong></span>
  </div>
</header>

<main class="map-wrap">
  <div class="map-frame">
    <img class="map-img" src="__MAP_IMAGE__" alt="Mapa de México">
    <svg class="map-overlay" viewBox="0 0 1000 643" preserveAspectRatio="xMidYMid meet" id="overlay"></svg>
  </div>
</main>

<div class="modal-backdrop" id="modal-backdrop">
  <div class="modal" id="modal" role="dialog" aria-modal="true">
    <div class="modal-header">
      <div>
        <div class="modal-coast-tag" id="modal-tag"><span id="modal-region">—</span></div>
        <div class="modal-title" id="modal-title">—</div>
        <div class="modal-period" id="modal-period">—</div>
      </div>
      <button class="modal-close" id="modal-close" aria-label="Cerrar">×</button>
    </div>
    <div class="tabbar" id="tabbar">
      <button class="tab active" data-tab="info">Descripción</button>
      <button class="tab" data-tab="vista">Vista</button>
      <button class="tab" data-tab="sea">Estado del mar</button>
      <button class="tab" data-tab="design">Diseño</button>
    </div>
    <div id="panes" style="overflow:hidden;">
      <div class="tab-pane pane-info active" id="pane-info"></div>
      <div class="tab-pane pane-vista" id="pane-vista"></div>
      <div class="tab-pane pane-sea" id="pane-sea"></div>
      <div class="tab-pane pane-design" id="pane-design"></div>
    </div>
  </div>
</div>

<script>
const DATA = __DATA__;
const SEA_ORDER  = __SEA_ORDER__;
const SEA_COLORS = __SEA_COLORS__;
const COLOR_MOSS = "#4B7158";
const COLOR_CORAL = "#D85D3F";
const MONTH_ABBR = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];

const overlay = document.getElementById("overlay");
const backdrop = document.getElementById("modal-backdrop");
const modalTitle = document.getElementById("modal-title");
const modalRegion = document.getElementById("modal-region");
const modalPeriod = document.getElementById("modal-period");
const modalTag = document.getElementById("modal-tag");

const nCoasts = Object.keys(DATA).length;
document.getElementById("coast-count").textContent =
  String(nCoasts).padStart(2,"0") + " / " + String(nCoasts).padStart(2,"0");

const SVG_NS = "http://www.w3.org/2000/svg";
Object.entries(DATA).forEach(([id, d]) => {
  const color = d.color === "moss" ? COLOR_MOSS : COLOR_CORAL;
  const g = document.createElementNS(SVG_NS, "g");
  g.setAttribute("class", "marker-group");
  g.setAttribute("transform", `translate(${d.x}, ${d.y})`);
  g.innerHTML = `
    <circle class="marker-pulse" cx="0" cy="0" r="8" fill="${color}" opacity="0.55"/>
    <circle class="marker-core" cx="0" cy="0" r="7" fill="#F4EDDF" stroke="${color}" stroke-width="2.5"/>
    <circle cx="0" cy="0" r="2.5" fill="${color}"/>
    <text class="marker-label" x="14" y="-6">${d.name}</text>
    <text class="marker-tag" x="14" y="8">${d.region.toUpperCase()} · ${d.num}</text>`;
  g.addEventListener("click", () => openModal(id));
  overlay.appendChild(g);
});

let current = null;
const rendered = {};

function openModal(id) {
  current = DATA[id];
  rendered.vista = rendered.sea = rendered.design = false;
  const d = current;
  modalTitle.textContent = d.name;
  modalRegion.textContent = d.region;
  modalTag.className = "modal-coast-tag" + (d.color === "moss" ? " moss" : "");
  modalPeriod.textContent = d.has_data
    ? `Periodo ${d.year_min} — ${d.year_max}  ·  ${d.n_records.toLocaleString()} días`
    : "Sin registros diarios para esta costa";

  buildVista(d); buildSea(d); buildDesign(d); buildInfo(d);
  activateTab("info");
  backdrop.classList.add("open");
}
function closeModal() { backdrop.classList.remove("open"); }
document.getElementById("modal-close").addEventListener("click", closeModal);
backdrop.addEventListener("click", e => { if (e.target === backdrop) closeModal(); });
document.addEventListener("keydown", e => { if (e.key === "Escape") closeModal(); });

document.getElementById("tabbar").addEventListener("click", e => {
  const btn = e.target.closest(".tab"); if (!btn) return;
  activateTab(btn.dataset.tab);
});
function activateTab(name) {
  document.querySelectorAll(".tab").forEach(t => t.classList.toggle("active", t.dataset.tab === name));
  document.querySelectorAll(".tab-pane").forEach(p => p.classList.remove("active"));
  document.getElementById("pane-" + name).classList.add("active");
  if (current && current.has_data && !rendered[name]) {
    requestAnimationFrame(() => {
      if (name === "vista")  renderVista(current);
      if (name === "sea")    renderSea(current);
      if (name === "design") renderDesign(current);
      rendered[name] = true;
    });
  }
}

const baseLayout = {
  margin: { t: 10, l: 50, r: 15, b: 42 },
  plot_bgcolor: "#fafafa", paper_bgcolor: "#fafafa",
  font: { family: "Manrope, sans-serif", size: 10, color: "#555" },
  xaxis: { gridcolor: "#e8e8e8", linecolor: "#ccc", zeroline: false },
  yaxis: { gridcolor: "#e8e8e8", linecolor: "#ccc", zeroline: false },
};
const cfg = { responsive: true, displayModeBar: false };

/* ----- VISTA ----- */
function buildVista(d) {
  const pane = document.getElementById("pane-vista");
  if (!d.has_data) {
    pane.classList.remove("pane-vista");
    pane.innerHTML = `<div class="empty-state"><div>Esta costa no tiene registros diarios.</div>
      <small>revisa wave_daily_summary en ${"__CATALOG__"}.gold</small></div>`;
    return;
  }
  pane.classList.add("pane-vista");
  const y0 = d.year_min, y1 = d.year_max;
  pane.innerHTML = `
    <div class="panel vista-filter">
      <div class="filter-head">
        <span class="filter-title">Rango de años</span>
        <span class="filter-readout"><span id="yr-readout">${y0} — ${y1}</span> <small>de ${y0}–${y1}</small></span>
      </div>
      <div class="range-wrap">
        <div class="range-track"><div class="range-fill" id="yr-fill"></div></div>
        <input type="range" id="yr-min" min="${y0}" max="${y1}" value="${y0}" step="1">
        <input type="range" id="yr-max" min="${y0}" max="${y1}" value="${y1}" step="1">
      </div>
    </div>
    <div class="kpi-col">
      <div class="kpi"><div class="kpi-value" id="kpi-mean">${d.kpi_mean.toFixed(2)}</div><div class="kpi-unit">m</div><div class="kpi-label">Oleaje Promedio</div></div>
      <div class="kpi"><div class="kpi-value" id="kpi-max">${d.kpi_max.toFixed(2)}</div><div class="kpi-unit">m</div><div class="kpi-label">Oleaje Máximo</div></div>
      <div class="kpi"><div class="kpi-value" id="kpi-wind">${d.kpi_wind != null ? d.kpi_wind.toFixed(2) : "—"}</div><div class="kpi-unit">m/s</div><div class="kpi-label">${d.wind_label}</div></div>
    </div>
    <div class="panel annual-panel"><div class="panel-title">Historial Anual de Oleaje Máximo</div><div id="chart-annual" class="chart"></div></div>
    <div class="panel seasonality-panel"><div class="panel-title">Estacionalidad: Oleaje Promedio vs Extremo</div><div id="chart-seasonality" class="chart"></div></div>
    <div class="panel power-panel"><div class="panel-title">Estacionalidad: Potencia Media del Oleaje</div><div id="chart-power" class="chart"></div></div>`;
  setupYearSlider(d);
}

function setupYearSlider(d) {
  const minEl = document.getElementById("yr-min");
  const maxEl = document.getElementById("yr-max");
  if (!minEl || !maxEl) return;
  const lo = d.year_min, hi = d.year_max;
  function refreshUI() {
    let a = parseInt(minEl.value, 10), b = parseInt(maxEl.value, 10);
    if (a > b) { if (document.activeElement === minEl) { b = a; maxEl.value = b; } else { a = b; minEl.value = a; } }
    document.getElementById("yr-readout").textContent = a + " — " + b;
    const span = (hi - lo) || 1;
    const left = (a - lo) / span * 100, right = (b - lo) / span * 100;
    const fill = document.getElementById("yr-fill");
    fill.style.left = left + "%"; fill.style.width = (right - left) + "%";
  }
  const onMove = () => { refreshUI(); renderVista(d); };
  minEl.addEventListener("input", onMove);
  maxEl.addEventListener("input", onMove);
  refreshUI();
}

function readWindow(d) {
  const minEl = document.getElementById("yr-min");
  const maxEl = document.getElementById("yr-max");
  if (!minEl || !maxEl) return [d.year_min, d.year_max];
  let a = parseInt(minEl.value, 10), b = parseInt(maxEl.value, 10);
  return [Math.min(a, b), Math.max(a, b)];
}

// Recombina los datos mensuales por año dentro de [y0, y1]. Promedio ponderado
// por días (n) para medias; máximo de máximos para extremos.
function filterMonthly(d, y0, y1) {
  const mby = d.monthly_by_year;
  if (!mby) {  // respaldo: usa el agregado completo
    return { months: d.monthly.months, mean_hs: d.monthly.mean_hs, max_hs: d.monthly.max_hs, mean_wind: d.monthly.mean_wind, mean_power: d.monthly.mean_power };
  }
  const mean = new Array(12).fill(null), max = new Array(12).fill(null), wind = new Array(12).fill(null), power = new Array(12).fill(null);
  for (let m = 0; m < 12; m++) {
    let sH = 0, sW = 0, sP = 0, n = 0, mx = null;
    for (let y = y0; y <= y1; y++) {
      const r = mby[y]; if (!r) continue;
      const cnt = r.n[m];
      if (cnt) {
        if (r.mean_hs[m] != null)    { sH += r.mean_hs[m] * cnt; }
        if (r.mean_wind[m] != null)  { sW += r.mean_wind[m] * cnt; }
        if (r.mean_power[m] != null) { sP += r.mean_power[m] * cnt; }
        n += cnt;
      }
      if (r.max_hs[m] != null) mx = (mx == null) ? r.max_hs[m] : Math.max(mx, r.max_hs[m]);
    }
    if (n) { mean[m] = sH / n; wind[m] = sW / n; power[m] = sP / n; }
    max[m] = mx;
  }
  return { months: [1,2,3,4,5,6,7,8,9,10,11,12], mean_hs: mean, max_hs: max, mean_wind: wind, mean_power: power };
}

function filterAnnual(d, y0, y1) {
  const years = [], max_hs = [], max_period = [], max_wind = [];
  d.annual.years.forEach((yy, i) => {
    if (yy >= y0 && yy <= y1) {
      years.push(yy);
      max_hs.push(d.annual.max_hs[i]);
      max_period.push(d.annual.max_period ? d.annual.max_period[i] : null);
      max_wind.push(d.annual.max_wind ? d.annual.max_wind[i] : null);
    }
  });
  return { years, max_hs, max_period, max_wind };
}

// KPIs recalculados dentro de la ventana de años (promedio ponderado por días).
function computeKPIs(d, y0, y1) {
  const mby = d.monthly_by_year;
  if (!mby) return { mean: d.kpi_mean, max: d.kpi_max, wind: d.kpi_wind };
  let sH = 0, sW = 0, n = 0, mx = null;
  for (let y = y0; y <= y1; y++) {
    const r = mby[y]; if (!r) continue;
    for (let m = 0; m < 12; m++) {
      const cnt = r.n[m];
      if (cnt) {
        if (r.mean_hs[m] != null)   sH += r.mean_hs[m] * cnt;
        if (r.mean_wind[m] != null) sW += r.mean_wind[m] * cnt;
        n += cnt;
      }
      if (r.max_hs[m] != null) mx = (mx == null) ? r.max_hs[m] : Math.max(mx, r.max_hs[m]);
    }
  }
  return { mean: n ? sH / n : null, max: mx, wind: n ? sW / n : null };
}

function renderVista(d) {
  const accent = d.color === "moss" ? COLOR_MOSS : COLOR_CORAL;
  const [y0, y1] = readWindow(d);
  const mo = filterMonthly(d, y0, y1);
  const an = filterAnnual(d, y0, y1);

  // KPIs dinámicos según el rango
  const k = computeKPIs(d, y0, y1);
  const setKpi = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = (v != null ? v.toFixed(2) : "—"); };
  setKpi("kpi-mean", k.mean);
  setKpi("kpi-max", k.max);
  setKpi("kpi-wind", k.wind);

  // Gráfica anual de oleaje máximo (primera fila), con periodo y viento en la etiqueta
  const sorted = [...an.max_hs].filter(v => v != null).sort((a, b) => b - a);
  const threshold = sorted.length ? sorted[Math.min(4, sorted.length - 1)] : Infinity;
  const labels = an.max_hs.map(v => (v != null && v >= threshold) ? v.toFixed(1) : "");
  const custom = an.years.map((_, i) => [
    an.max_period[i] != null ? an.max_period[i].toFixed(1) : "—",
    an.max_wind[i]   != null ? an.max_wind[i].toFixed(1)   : "—",
  ]);
  Plotly.newPlot("chart-annual", [{
    x: an.years, y: an.max_hs, type: "bar", marker: { color: accent },
    text: labels, textposition: "outside", textfont: { size: 9 },
    customdata: custom,
    hovertemplate: "Año %{x}<br>Hs máx: %{y:.2f} m<br>Periodo máx: %{customdata[0]} s<br>Viento máx: %{customdata[1]} m/s<extra></extra>"
  }], { ...baseLayout,
    xaxis: { ...baseLayout.xaxis, title: { text: "Año", font: { size: 10 } } },
    yaxis: { ...baseLayout.yaxis, title: { text: "Altura Máxima (m)", font: { size: 10 } } },
    bargap: 0.18 }, cfg);

  // Estacionalidad: oleaje promedio vs extremo
  Plotly.newPlot("chart-seasonality", [
    { x: mo.months, y: mo.max_hs, name: "Extremo", mode: "lines+markers",
      line: { color: accent, width: 2 }, marker: { size: 6 },
      hovertemplate: "Mes %{x}<br>Hs máx: %{y:.2f} m<extra></extra>" },
    { x: mo.months, y: mo.mean_hs, name: "Promedio", mode: "lines+markers",
      line: { color: "#1f3a5f", width: 2 }, marker: { size: 6 },
      hovertemplate: "Mes %{x}<br>Hs prom: %{y:.2f} m<extra></extra>" }
  ], { ...baseLayout,
    xaxis: { ...baseLayout.xaxis, title: { text: "Mes", font: { size: 10 } }, dtick: 1,
             tickvals: mo.months, ticktext: mo.months.map(m => MONTH_ABBR[m-1]) },
    yaxis: { ...baseLayout.yaxis, title: { text: "Altura del Oleaje (m)", font: { size: 10 } } },
    legend: { orientation: "h", y: 1.14, x: 0, font: { size: 10 } } }, cfg);

  // Estacionalidad: potencia media del oleaje
  Plotly.newPlot("chart-power", [
    { x: mo.months, y: mo.mean_power, name: "Potencia", mode: "lines+markers",
      line: { color: accent, width: 2 }, marker: { size: 6 },
      fill: "tozeroy", fillcolor: accent + "22",
      hovertemplate: "Mes %{x}<br>Potencia: %{y:.1f} kW/m<extra></extra>" }
  ], { ...baseLayout,
    xaxis: { ...baseLayout.xaxis, title: { text: "Mes", font: { size: 10 } }, dtick: 1,
             tickvals: mo.months, ticktext: mo.months.map(m => MONTH_ABBR[m-1]) },
    yaxis: { ...baseLayout.yaxis, title: { text: "Potencia (kW/m)", font: { size: 10 } }, rangemode: "tozero" } }, cfg);
}

/* ----- ESTADO DEL MAR ----- */
function buildSea(d) {
  const pane = document.getElementById("pane-sea");
  if (!d.seastate) {
    pane.classList.remove("pane-sea");
    pane.innerHTML = `<div class="empty-state"><div>Sin clasificación mensual para esta costa.</div>
      <small>wave_monthly_classification</small></div>`;
    return;
  }
  pane.classList.add("pane-sea");
  pane.innerHTML = `<div class="panel"><div class="panel-title">Composición mensual del estado del mar (% de días, promedio multianual)</div><div id="chart-sea" class="chart"></div></div>`;
}
function renderSea(d) {
  if (!d.seastate) return;
  const traces = SEA_ORDER.map(cls => ({
    x: d.seastate.months, y: d.seastate.series[cls], name: cls, type: "bar",
    marker: { color: SEA_COLORS[cls] },
    hovertemplate: cls + "<br>Mes %{x}: %{y:.1f}%<extra></extra>"
  }));
  Plotly.newPlot("chart-sea", traces, { ...baseLayout,
    barmode: "stack",
    margin: { t: 10, l: 50, r: 15, b: 60 },
    xaxis: { ...baseLayout.xaxis, title: { text: "Mes", font: { size: 10 } }, dtick: 1,
             tickvals: d.seastate.months, ticktext: d.seastate.months.map(m => MONTH_ABBR[m-1]) },
    yaxis: { ...baseLayout.yaxis, title: { text: "% de días", font: { size: 10 } }, range: [0, 100] },
    legend: { orientation: "h", y: -0.22, x: 0, font: { size: 9 } } }, cfg);
}

/* ----- DISEÑO (forecast 50/100 años) ----- */
function buildDesign(d) {
  const pane = document.getElementById("pane-design");
  if (!d.forecast) {
    pane.classList.remove("pane-design");
    pane.innerHTML = `<div class="empty-state">
      <div>Aún no hay predicciones de altura de diseño para esta costa.</div>
      <small>la tabla significant_wave_height_forecast está vacía</small></div>`;
    return;
  }
  pane.classList.add("pane-design");
  const f = d.forecast;
  const card = (titulo, o) => `
    <div class="design-card">
      <div class="dc-head">${titulo}</div>
      <div class="dc-mean">${o.mean != null ? o.mean.toFixed(2) : "—"} <small>m</small></div>
      <div class="dc-ci">IC 95%: ${o.lower != null ? o.lower.toFixed(2) : "—"} — ${o.upper != null ? o.upper.toFixed(2) : "—"} m</div>
    </div>`;
  const ts = f.timestamp ? `Predicción: ${f.timestamp}` : "";
  pane.innerHTML = `
    <div class="design-cards">
      ${card("Altura significante · retorno 50 años", f.h50)}
      ${card("Altura significante · retorno 100 años", f.h100)}
    </div>
    <div class="panel design-chart-panel">
      <div class="panel-title">Altura de diseño con intervalo de confianza al 95% ${ts ? "· " + ts : ""}</div>
      <div id="chart-design" class="chart"></div>
    </div>`;
}
function renderDesign(d) {
  if (!d.forecast) return;
  const accent = d.color === "moss" ? COLOR_MOSS : COLOR_CORAL;
  const f = d.forecast;
  const x = ["50 años", "100 años"];
  const means = [f.h50.mean, f.h100.mean];
  const up = [f.h50.upper - f.h50.mean, f.h100.upper - f.h100.mean];
  const lo = [f.h50.mean - f.h50.lower, f.h100.mean - f.h100.lower];
  Plotly.newPlot("chart-design", [{
    x: x, y: means, type: "scatter", mode: "markers",
    marker: { size: 18, color: accent, line: { color: "white", width: 2 } },
    error_y: { type: "data", symmetric: false, array: up, arrayminus: lo,
               color: accent, thickness: 2, width: 14 },
    hovertemplate: "%{x}<br>Hs: %{y:.2f} m<extra></extra>"
  }], { ...baseLayout,
    margin: { t: 10, l: 55, r: 20, b: 42 },
    xaxis: { ...baseLayout.xaxis, title: { text: "Periodo de retorno", font: { size: 10 } } },
    yaxis: { ...baseLayout.yaxis, title: { text: "Altura significante (m)", font: { size: 10 } }, rangemode: "tozero" } }, cfg);
}

/* ----- DESCRIPCIÓN ----- */
function buildInfo(d) {
  const pane = document.getElementById("pane-info");
  const paras = (d.description || []).map(p => `<p>${p}</p>`).join("");
  const img = d.image
    ? `<div class="info-image"><img src="${d.image}" alt="${d.name}"></div>`
    : `<div class="info-image"><div class="ph">Imagen no disponible<br>${d.name}, ${d.state}</div></div>`;
  pane.innerHTML = `
    ${img}
    <div class="info-text ${d.color === "moss" ? "moss" : ""}">
      <div class="info-state">${d.state} · ${d.region}</div>
      ${paras}
    </div>`;
}
</script>
</body>
</html>
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Inyectar, guardar y mostrar

# COMMAND ----------

html_out = (
    HTML_TEMPLATE
    .replace("__MAP_IMAGE__", MAP_IMAGE_URI)
    .replace("__DATA__", json.dumps(coast_data, ensure_ascii=False))
    .replace("__SEA_ORDER__", json.dumps(SEA_STATE_ORDER, ensure_ascii=False))
    .replace("__SEA_COLORS__", json.dumps(SEA_STATE_COLORS, ensure_ascii=False))
    .replace("__SOURCE__", SOURCE_LABEL)
    .replace("__CATALOG__", CATALOG)
)

parent_dir = os.path.dirname(HTML_OUTPUT_PATH)
os.makedirs(parent_dir, exist_ok=True)
with open(HTML_OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write(html_out)

if os.path.isfile(HTML_OUTPUT_PATH):
    size_kb = os.path.getsize(HTML_OUTPUT_PATH) / 1024
    print(f"OK · guardado ({size_kb:.1f} KB) en:\n  {HTML_OUTPUT_PATH}")
else:
    print("ERROR · no se escribió el archivo.")

# COMMAND ----------

displayHTML(html_out)
import os

OUT_NAME = "atlas_costero.html"

out_path = os.path.join(os.getcwd(), OUT_NAME)
with open(out_path, "w", encoding="utf-8") as f:
    f.write(html_out)

print("HTML guardado junto al notebook en:", out_path)