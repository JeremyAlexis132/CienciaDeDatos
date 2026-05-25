import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--ambiente")
args = parser.parse_args()

ambiente = args.ambiente
print(ambiente)

spark.sql(
    f"""
    INSERT OVERWRITE TABLE cor_{ambiente}.gold.wave_monthly_classification
    WITH classifications AS (
        SELECT 'Mar calmado' AS wave_classification UNION ALL
        SELECT 'Mar suave' UNION ALL
        SELECT 'Mar dinámico' UNION ALL
        SELECT 'Mar agitado' UNION ALL
        SELECT 'Mar fuerte' UNION ALL
        SELECT 'Mar peligroso' UNION ALL
        SELECT 'Mar extremo'
    ),

    base AS (
        SELECT
            year,
            date_format(datetime, 'MM') AS month,
            coast_name,
            wave_classification
        FROM cor_{ambiente}.silver.swell_clasification
    ),

    months AS (
        SELECT DISTINCT
            year,
            month,
            coast_name
        FROM base
    ),

    total_records AS (
        SELECT
            year,
            month,
            coast_name,
            COUNT(*) AS total_records
        FROM base
        GROUP BY year, month, coast_name
    ),

    classified_records AS (
        SELECT
            year,
            month,
            coast_name,
            wave_classification,
            COUNT(*) AS records_by_classification
        FROM base
        GROUP BY
            year,
            month,
            coast_name,
            wave_classification
    )

    SELECT
        m.year,
        m.month,
        m.coast_name,
        c.wave_classification,
        CAST(
            ROUND(
                COALESCE(cr.records_by_classification, 0) * 100.0 / tr.total_records,
                2
            ) AS FLOAT
        ) AS percentage
    FROM months m
    CROSS JOIN classifications c
    INNER JOIN total_records tr
        ON  m.year = tr.year
        AND m.month = tr.month
        AND m.coast_name = tr.coast_name
    LEFT JOIN classified_records cr
        ON  m.year = cr.year
        AND m.month = cr.month
        AND m.coast_name = cr.coast_name
        AND c.wave_classification = cr.wave_classification
    """
)