USE ROLE TRANSFORMER;
USE DATABASE NYC_TAXI;
USE SCHEMA RAW;

-- ============================================================
-- FILE FORMATS : indique à snowflake que les fichiers sont en parquet ou csv
-- ============================================================

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT
    TYPE = PARQUET;

CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

-- ============================================================
-- STAGE
-- ============================================================

CREATE STAGE IF NOT EXISTS TAXI_STAGE
    FILE_FORMAT = PARQUET_FORMAT
    COPY_OPTIONS = (ON_ERROR = 'CONTINUE');

-- ============================================================
-- RAW TABLE : YELLOW TRIP DATA
-- ============================================================

CREATE TABLE IF NOT EXISTS YELLOW_TRIPDATA (
    vendorid                 INT,
    tpep_pickup_datetime     TIMESTAMP_NTZ,
    tpep_dropoff_datetime    TIMESTAMP_NTZ,
    passenger_count          INT,
    trip_distance            DECIMAL,
    ratecodeid               INT,
    store_and_fwd_flag       TEXT,
    pulocationid             INT,
    dolocationid             INT,
    payment_type             INT,
    fare_amount              DECIMAL,
    extra                    DECIMAL,
    mta_tax                  DECIMAL,
    tip_amount               DECIMAL,
    tolls_amount             DECIMAL,
    improvement_surcharge    DECIMAL,
    total_amount             DECIMAL,
    congestion_surcharge     DECIMAL,
    airport_fee              DECIMAL,
    cbd_congestion_fee       DECIMAL,
    _source_file             TEXT,
    _loaded_at               TIMESTAMP_NTZ
);

-- ============================================================
-- RAW TABLE : TAXI ZONE LOOKUP
-- ============================================================

CREATE TABLE IF NOT EXISTS TAXI_ZONE_LOOKUP (
    locationid INT,
    borough TEXT,
    zone TEXT,
    service_zone TEXT,
    _source_file TEXT,
    _loaded_at TIMESTAMP_NTZ
);

-- ============================================================
-- 4. LOAD PARQUET -> RAW
-- ============================================================
-- Le Parquet est lu comme une structure VARIANT ($1).
--
-- Les timestamps du fichier sont exprimés en microsecondes
-- depuis Unix epoch. On divise donc par 1 000 000 avant
-- conversion en TIMESTAMP_NTZ.
--
-- METADATA$FILENAME permet de conserver le nom du fichier source.

COPY INTO YELLOW_TRIPDATA
(
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    _source_file,
    _loaded_at
)
FROM
(
    SELECT
        $1:VendorID::INT,

        TO_TIMESTAMP_NTZ(
            $1:tpep_pickup_datetime::NUMBER / 1000000
        ),

        TO_TIMESTAMP_NTZ(
            $1:tpep_dropoff_datetime::NUMBER / 1000000
        ),
        $1:passenger_count::INT,
        $1:trip_distance::DECIMAL,
        $1:RatecodeID::INT,
        $1:store_and_fwd_flag::TEXT,
        $1:PULocationID::INT,
        $1:DOLocationID::INT,
        $1:payment_type::INT,
        $1:fare_amount::DECIMAL,
        $1:extra::DECIMAL,
        $1:mta_tax::DECIMAL,
        $1:tip_amount::DECIMAL,
        $1:tolls_amount::DECIMAL,
        $1:improvement_surcharge::DECIMAL,
        $1:total_amount::DECIMAL,
        $1:congestion_surcharge::DECIMAL,
        $1:Airport_fee::DECIMAL,
        $1:cbd_congestion_fee::DECIMAL,

        METADATA$FILENAME,

        CURRENT_TIMESTAMP()

    FROM @TAXI_STAGE
);