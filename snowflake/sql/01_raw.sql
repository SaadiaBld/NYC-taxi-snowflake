-- Exécuter avec le rôle TRANSFORMER après 00_infrastructure.sql.
USE ROLE TRANSFORMER;
USE DATABASE NYC_TAXI;
USE SCHEMA RAW;

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT
    TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS TAXI_STAGE
    FILE_FORMAT = PARQUET_FORMAT;

CREATE TABLE IF NOT EXISTS YELLOW_TRIPDATA (
    vendorid                  NUMBER(10, 0),
    tpep_pickup_datetime      TIMESTAMP_NTZ,
    tpep_dropoff_datetime     TIMESTAMP_NTZ,
    passenger_count           NUMBER(10, 0),
    trip_distance             NUMBER(18, 6),
    ratecodeid                NUMBER(10, 0),
    store_and_fwd_flag        VARCHAR(1),
    pulocationid              NUMBER(10, 0),
    dolocationid              NUMBER(10, 0),
    payment_type              NUMBER(10, 0),
    fare_amount               NUMBER(18, 6),
    extra                     NUMBER(18, 6),
    mta_tax                   NUMBER(18, 6),
    tip_amount                NUMBER(18, 6),
    tolls_amount              NUMBER(18, 6),
    improvement_surcharge     NUMBER(18, 6),
    total_amount              NUMBER(18, 6),
    congestion_surcharge      NUMBER(18, 6),
    airport_fee               NUMBER(18, 6),
    cbd_congestion_fee        NUMBER(18, 6),
    _source_file              VARCHAR(255),
    _loaded_at                TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS TAXI_ZONE_LOOKUP (
    locationid    NUMBER(10, 0),
    borough       VARCHAR(100),
    zone          VARCHAR(255),
    service_zone  VARCHAR(100),
    _source_file  VARCHAR(255),
    _loaded_at    TIMESTAMP_NTZ
);