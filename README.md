# Análisis Meteorológico Marino - Sabancuy, Campeche 🌊📊

Este proyecto contiene un análisis exploratorio y visualización de datos de condiciones meteoceanográficas (viento y oleaje) de Sabancuy, Campeche (1979 - 2019). Está diseñado en Python utilizando Jupyter Notebooks, con un enfoque en alto rendimiento usando la librería **Polars** para procesamiento de grandes volúmenes de datos (~350k registros) y **Plotly** para visualizaciones interactivas de ingeniería.

## 📈 Gráficos generados
El notebook principal incluye la generación de:
- Histogramas de distribución con ajuste a **Distribución de Gumbel** para valores extremos.
- Curvas de Probabilidad de Excedencia.
- Series de tiempo a largo plazo (Hidrogramas).
- **Rosas de Vientos y Rosas de Oleaje** (con sectorización técnica a 11.25°).
- Matrices de correlación (Viento vs Oleaje).

---

## ⚙️ Estructura del Proyecto

```text
📁 CienciaDeDatos/
├── 📁 src/
│   ├── 📁 data/           # Colocar aquí el archivo DataSetLimpioSabancuy.csv
│   └── 📁 Notebook/
│       └── DataExtraction.ipynb   # Notebook principal del análisis
├── 📄 .gitignore
├── 📄 requirements.txt    # Dependencias del proyecto
└── 📄 README.md
```

---

## 🚀 Instalación y Uso

Para ejecutar este proyecto en tu entorno local, sigue estos pasos:

### 1. Clonar el repositorio
```bash
git clone <url-de-tu-repositorio>
cd CienciaDeDatos
```

### 2. Crear y activar un entorno virtual
Se recomienda usar un entorno virtual para no generar conflictos con otras instalaciones de Python.

**En Windows:**
```powershell
python -m venv venv
.\venv\Scripts\activate
```

**En macOS/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar las dependencias
Asegúrate de que tu entorno virtual esté activo `(venv)` y ejecuta:

```bash
pip install -r requirements.txt
```

### 4. Configurar Jupyter Kernel
Para asegurarte de que VS Code / Jupyter usen tu entorno virtual recién creado:
```bash
python -m ipykernel install --user --name=venv-cienciadatos --display-name "Python (venv-cienciadatos)"
```

### 5. Colocar los datos y ejecutar
- Coloca tu archivo `DataSetLimpioSabancuy.csv` dentro de la carpeta `src/data/`.
- Abre el archivo `src/Notebook/DataExtraction.ipynb` en VS Code o Jupyter Lab.
- Asegúrate de seleccionar el kernel `"Python (venv-cienciadatos)"` en la esquina superior derecha.
- ¡Ejecuta todas las celdas!

---

## 📦 Dependencias (requirements.txt)
Si aún no tienes tu archivo `requirements.txt`, puedes crearlo con el siguiente contenido:

```text
polars
pandas
pyarrow
numpy
scipy
plotly
matplotlib
seaborn
kaleido
jupyterlab
ipykernel
```

---
*Desarrollado con fines de investigación e ingeniería costera.*