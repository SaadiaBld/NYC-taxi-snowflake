-- Le pourcentage de trajets rejetés ne doit pas dépasser le seuil du DAG.
SELECT COALESCE(
    100.0 * COUNT_IF(rejection_reason IS NOT NULL) / NULLIF(COUNT(*), 0)
        <= {{ params.max_rejection_pct }}
    AND COUNT(*) > 0,
    FALSE
)
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
WHERE source_file_month = '{{ ds }}'::date;