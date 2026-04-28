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
        ) USING DELTA
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
                COMMENT 'Nombre de la costa a la que corresponden las métricas de olas. Esta información se extrae del nombre del archivo fuente',
            datetime TIMESTAMP
                COMMENT 'Fecha y hora de la medición',
            year INTEGER
                COMMENT 'Año de la medición, extraído de la fecha y hora',
            wind_speed_ms FLOAT
                COMMENT 'Velocidad del viento en metros por segundo',
            wind_direction_deg FLOAT
                COMMENT 'Dirección del viento en grados',
            wind_u FLOAT
                COMMENT 'Componente u de la velocidad del viento, calculado a partir de la velocidad y dirección del viento',
            wind_v FLOAT
                COMMENT 'Componente v de la velocidad del viento, calculado a partir de la velocidad y dirección del viento',
            wave_height_m FLOAT
                COMMENT 'Altura de las olas en metros',
            wave_direction_deg FLOAT
                COMMENT 'Dirección de las olas en grados',
            wave_u FLOAT
                COMMENT 'Componente u de la altura de las olas, calculado a partir de la altura y dirección de las olas',
            wave_v FLOAT
                COMMENT 'Componente v de la altura de las olas, calculado a partir de la altura y dirección de las olas',
            wave_period_s FLOAT
                COMMENT 'Periodo de las olas en segundos',
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
            COMMENT 'Tabla con los registros de métricas de olas que no cumplieron con los criterios de calidad durante el proceso de transformación. 
                Cada registro contiene los datos crudos originales, la ruta del archivo fuente, el motivo del error, las marcas de tiempo de ingesta y 
                transformación, y un indicador de si el problema ha sido resuelto. Esta tabla permite realizar un seguimiento de los registros problemáticos 
                y facilita su revisión y corrección para su posterior inclusión en la tabla silver.swell_metrics.'
    """
)

# Tablas de la capa Gold

# Tablas de la capa ML