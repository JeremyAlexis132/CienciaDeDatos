import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

# Tablas de la capa Bronze
spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.bronze.raw_swell_metrics (
            data STRING
                COMMENT 'Registro original como fue leido del archivo csv',
            source_file STRING
                COMMENT 'Ruta del archivo fuente del cual se extrajo el registro',
            ingestion_timestamp TIMESTAMP
                COMMENT 'Fecha y hora en que el registro fue ingresado a la tabla bronze.raw_swell_metrics'
        ) 
        USING DELTA
        COMMENT 'Tabla para almacenar los datos crudos de métricas de olas, tal como fueron leídos de los archivos CSV. 
            Cada registro contiene el dato original, la ruta del archivo fuente y la marca de tiempo de ingesta. 
            Esta tabla sirve como punto de partida para el procesamiento y transformación de los datos en las capas posteriores.'
    """
)

# Tablas de la capa Silver
spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.silver.swell_metrics (
            id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
                COMMENT 'ID de registro único generado automáticamente. Se utiliza para poder identificar en las tablas de la 
                    capa Gold el registro original del cual se extrajo la información.',
            coast_name STRING
                COMMENT 'Nombre de la costa a la que corresponden las métricas de olas. Esta información se extrae del nombre del archivo fuente, 
                    El nombre tiene el formato limpio con espacios y mayúsculas',
            datetime TIMESTAMP
                COMMENT 'Fecha y hora de la medición',
            year INTEGER
                COMMENT 'Año de la medición, extraído de la fecha y hora',
            wind_speed_ms FLOAT
                COMMENT 'Velocidad del viento en metros por segundo',
            wind_direction_deg FLOAT
                COMMENT 'Dirección del viento en grados',
            wind_cos_direction FLOAT
                COMMENT 'Coseno de la direccion del viento se usa para evitar problemas de linealidad en los modelos de ML',
            wind_sin_direction FLOAT
                COMMENT 'Seno de la direccion del viento se usa para evitar problemas de linealidad en los modelos de ML',
            wave_height_m FLOAT
                COMMENT 'Altura de las olas en metros',
            wave_direction_deg FLOAT
                COMMENT 'Dirección de las olas en grados',
            wave_cos_direction FLOAT
                COMMENT 'Coseno de la direccion de la ola se usa para evitar problemas de linealidad en los modelos de ML',
            wave_sin_direction FLOAT
                COMMENT 'Seno de la direccion de la ola se usa para evitar problemas de linealidad en los modelos de ML',
            wave_period_s FLOAT
                COMMENT 'Periodo de las olas en segundos',
            wave_energy FLOAT
                COMMENT 'Energía de las olas, calculada como (1/8) * gravedad * altura^2',
            wave_steepness FLOAT
                COMMENT 'Factor para determinar la estabilidad de las olas.',
            wave_classification STRING
                COMMENT 'Clasificación del estado del mar para la medición, basada en las métricas de olas. Puede ser .',
            ingestion_timestamp TIMESTAMP
                COMMENT 'Fecha y hora en que el registro fue ingresado a la tabla bronze.raw_swell_metrics',
            source_file STRING
                COMMENT 'Ruta del archivo fuente del cual se extrajo el registro original en la tabla bronze.raw_swell_metrics',
            transformation_timestamp TIMESTAMP
                COMMENT 'Fecha y hora en que el registro fue transformado y cargado en esta tabla silver.swell_metrics'
        )
        USING DELTA
        PARTITIONED BY (coast_name, year)
        COMMENT 'Tabla para almacenar las métricas de olas transformadas y listas para su uso en análisis posteriores. 
            Se partitiona por nombre de costa y año para mejorar el rendimiento de las consultas. Los registros en esta tabla se generan 
            a partir de los datos crudos en la tabla bronze.raw_swell_metrics, aplicando las transformaciones y calidad de datos.'
    """
)

spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.silver.quarantine_swell_metrics (
            data STRING
                COMMENT 'Datos crudos de las métricas de olas que no cumplieron con los criterios de calidad, son una copia del registro original 
                    de la tabla bronze.raw_swell_metrics',
            source_file STRING
                COMMENT 'Ruta del archivo fuente del cual se extrajo el registro original en la tabla bronze.raw_swell_metrics',
            error_reason STRING
                COMMENT 'Motivo por el cual el registro no cumplió con los criterios de calidad',
            ingestion_timestamp TIMESTAMP
                COMMENT 'Fecha y hora en que el registro fue ingresado a la tabla bronze.raw_swell_metrics',
            transformation_timestamp TIMESTAMP
                COMMENT 'Fecha y hora en que el registro fue transformado y cargado en esta tabla silver.quarantine_swell_metrics',
            resolved BOOLEAN
                COMMENT 'Indica si el registro ha sido revisado y corregido para cumplir con los criterios de calidad. Inicialmente es FALSE, 
                    y se debe actualizar a TRUE una vez que se haya resuelto el problema que causó la cuarentena del registro.'
        )
        USING DELTA
        COMMENT 'Tabla con los registros de métricas de olas que no cumplieron con los criterios de calidad durante el proceso de transformación. 
            Cada registro contiene los datos crudos originales, la ruta del archivo fuente, el motivo del error, las marcas de tiempo de ingesta y 
            transformación, y un indicador de si el problema ha sido resuelto. Esta tabla permite realizar un seguimiento de los registros problemáticos 
            y facilita su revisión y corrección para su posterior inclusión en la tabla silver.swell_metrics.'
    """
)

spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.silver.swell_clasification (
            id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
                COMMENT 'ID de registro único se utiliza para relacionar con la tabla silver.swell_metrics.',
            coast_name STRING
                COMMENT 'Nombre de la costa a la que corresponden las métricas de olas.',
            datetime TIMESTAMP
                COMMENT 'Fecha y hora de la medición',
            year INTEGER
                COMMENT 'Año de la medición, extraído de la fecha y hora',
            wave_classification STRING
                COMMENT 'Clasificación del estado del mar para la medición, basada en las métricas de olas. Puede ser "Mar calmado", "Mar suave", 
                    "Mar dinámico", "Mar agitado", "Mar fuerte", "Mar peligroso" o "Mar extremo".',
            classification_timestamp TIMESTAMP
                COMMENT 'Fecha y hora en que se realizó la clasificación del estado del mar para esta medición.'
        )
        USING DELTA
        PARTITIONED BY (coast_name, year)
        COMMENT 'Tabla para almacenar la clasificación del estado del mar para cada medición de olas. 
            Se partitiona por nombre de costa y año para mejorar el rendimiento de las consultas. Los registros en esta tabla se generan a partir de los valores
            de swell_metrics, aplicando modelos de clasificacion'
    """
)

# Tablas de la capa Gold
spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.gold.wave_daily_summary (
            date DATE
                COMMENT 'Fecha de la medición, sin la hora.',
            coast_name STRING
                COMMENT 'Nombre de la costa a la que corresponden las métricas de olas.',
            max_wave_height_m FLOAT
                COMMENT 'Altura máxima de las olas en metros para la fecha y costa especificadas.',
            max_wave_period_s FLOAT
                COMMENT 'Periodo de la ola más alta en segundos para la fecha y costa especificadas.',
            max_wave_direction_deg FLOAT
                COMMENT 'Dirección de la ola más alta en grados para la fecha y costa especificadas.',
            max_wave_wind_speed_ms FLOAT
                COMMENT 'Velocidad del viento en metros por segundo de la ola más alta para la fecha y costa especificadas.',
            max_wave_wind_direction_deg FLOAT
                COMMENT 'Dirección del viento en grados de la ola más alta para la fecha y costa especificadas.',
            min_wave_height_m FLOAT
                COMMENT 'Altura mínima de las olas en metros para la fecha y costa especificadas.',
            min_wave_period_s FLOAT
                COMMENT 'Periodo de la ola más baja en segundos para la fecha y costa especificadas.',
            min_wave_direction_deg FLOAT
                COMMENT 'Dirección de la ola más baja en grados para la fecha y costa especificadas.',
            min_wave_wind_speed_ms FLOAT
                COMMENT 'Velocidad del viento en metros por segundo de la ola más baja para la fecha y costa especificadas.',
            min_wave_wind_direction_deg FLOAT
                COMMENT 'Dirección del viento en grados de la ola más baja para la fecha y costa especificadas.',
            avg_wave_height_m FLOAT
                COMMENT 'Altura promedio de las olas en metros para la fecha y costa especificadas.'
        )
        USING DELTA
        COMMENT 'Tabla con el resumen diario de las métricas de olas para cada costa. Cada registro contiene la fecha, el nombre de la costa, 
            y las métricas agregadas como la altura máxima, mínima y promedio de las olas, el periodo de la ola más alta y más baja, la dirección de la ola más alta, 
            y la velocidad del viento asociada a la ola más alta. Esta tabla se genera a partir de los datos transformados en la tabla silver.swell_metrics y se 
            utiliza para análisis y visualizaciones a nivel diario.'
    """
)

spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS cor_{ambiente}.gold.wave_monthly_classification (
            year INTEGER
                COMMENT 'Año de la medición.',
            month STRING
                COMMENT 'Mes de la medición.',
            coast_name STRING
                COMMENT 'Nombre de la costa a la que corresponden las métricas de olas.',
            wave_classification STRING
                COMMENT 'Clasificación general del estado del mar para el mes y costa especificados, basada en las métricas de olas. 
                    Puede ser "Calm", "Moderate" o "Rough" dependiendo de los valores agregados de altura, periodo y dirección de las olas.',
            percentage FLOAT
                COMMENT 'Porcentaje de días en el mes que corresponden a la clasificación general del estado del mar para la costa especificada.'
        )
        USING DELTA
        COMMENT 'Tabla con la clasificación mensual del estado del mar para cada costa. Cada registro contiene el año, mes, nombre de la costa, 
            la clasificación general del estado del mar basada en las métricas de olas, y el porcentaje de días en el mes que corresponden a esa clasificación. 
            Esta tabla se genera agregando la columna clasificacion de la tabla silver.swell_metrics.'
    """
)

# Tablas de la capa ML