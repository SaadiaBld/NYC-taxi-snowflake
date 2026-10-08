-- Aucun trip_sk ne doit apparaître plusieurs fois pour le mois traité.
SELECT COUNT(*) = 0
FROM (
    SELECT trip_sk
    FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__ENRICHED
    WHERE source_file_month = '{{ ds }}'::date
    GROUP BY trip_sk
    HAVING COUNT(*) > 1
);