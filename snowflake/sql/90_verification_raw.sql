-- Vérifier la présence des deux tables RAW et de leurs noms exacts.
SHOW TABLES IN SCHEMA NYC_TAXI.RAW;

-- Nombre de lignes et date du chargement, ventilés par fichier source.
SELECT
    _source_file,
    COUNT(*) AS row_count,
    MIN(_loaded_at) AS first_loaded_at,
    MAX(_loaded_at) AS last_loaded_at,
    COUNT_IF(_source_file IS NULL OR _loaded_at IS NULL) AS missing_metadata_rows
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
GROUP BY _source_file
ORDER BY _source_file;

-- Les montants doivent conserver leurs décimales.
SELECT fare_amount, total_amount
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
WHERE fare_amount IS NOT NULL OR total_amount IS NOT NULL
LIMIT 10;

-- La table de zones TLC contient 265 lignes et aucun identifiant de zone en double.
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT locationid) AS distinct_location_count,
    COUNT_IF(locationid IS NULL) AS null_location_count
FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP;