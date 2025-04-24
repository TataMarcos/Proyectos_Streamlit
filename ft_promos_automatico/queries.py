check_fechas_unicas = '''
WITH DUPLICADOS AS
    (
    SELECT
        EVENTO_ID,
        COUNT(*)
    FROM
        (
        SELECT
            DISTINCT
            EVENTO_ID,
            TO_VARCHAR(PROM_FECHA_INICIO, 'YYYY-MM') AS PERIODO_INICIO,
            TO_VARCHAR(PROM_FECHA_FIN, 'YYYY-MM') AS PERIODO_FIN
        FROM
            MSTRDB.DWH.FT_PROMOS
        WHERE
            PROM_FECHA_INICIO >= '2024-03-01'
        ORDER BY
            EVENTO_ID DESC
        )
    GROUP BY
        ALL
    HAVING
        COUNT(*) > 2
    )

    SELECT
        DISTINCT
        EVENTO_ID,
        TO_VARCHAR(PROM_FECHA_INICIO, 'YYYY-MM') AS PERIODO_INICIO,
        TO_VARCHAR(PROM_FECHA_FIN, 'YYYY-MM') AS PERIODO_FIN
    FROM
        MSTRDB.DWH.FT_PROMOS
    WHERE
        EVENTO_ID IN (SELECT EVENTO_ID FROM DUPLICADOS)
    ORDER BY
        EVENTO_ID DESC
'''