from __future__ import annotations

import logging
import tempfile
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.sdk import dag, get_current_context, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


CONNECTION_ID = "snowflake_nyc_taxi"
RAW_TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
RAW_STAGE = "NYC_TAXI.RAW.TAXI_STAGE"
PARQUET_FORMAT = "NYC_TAXI.RAW.PARQUET_FORMAT"
TRIPDATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"

COPY_SQL = f"""
    COPY INTO {RAW_TABLE}
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
            TO_TIMESTAMP_NTZ($1:tpep_pickup_datetime::NUMBER / 1000000),
            TO_TIMESTAMP_NTZ($1:tpep_dropoff_datetime::NUMBER / 1000000),
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
        FROM @{RAW_STAGE}/yellow_tripdata_{{month}}.parquet
            (FILE_FORMAT => '{PARQUET_FORMAT}')
    )
    FORCE = TRUE
    ON_ERROR = 'ABORT_STATEMENT'
"""


def download_trip_file(month: str, destination: Path) -> None:
    """Download one TLC Parquet file and leave no partial file on failure."""
    partial_path = destination.with_suffix(destination.suffix + ".part")
    url = TRIPDATA_URL.format(month=month)

    try:
        with requests.get(url, stream=True, timeout=(15, 300)) as response:
            response.raise_for_status()
            with partial_path.open("wb") as output_file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output_file.write(chunk)

        if partial_path.stat().st_size == 0:
            raise ValueError(f"Le fichier téléchargé est vide : {url}")

        partial_path.replace(destination)
    finally:
        partial_path.unlink(missing_ok=True)


@dag(
    dag_id="load_yellow_trips",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["snowflake", "raw", "monthly"],
    doc_md=(
        "Charge le fichier TLC Yellow Taxi du mois correspondant à la date logique. "
        "Chaque mois doit être lancé explicitement avec une date logique Airflow."
    ),
)
def load_yellow_trips():
    ''' '''
    @task(task_id="download_and_load_month")
    def download_and_load_month() -> None:
        logical_date = get_current_context().get("logical_date")
        if not logical_date:
            raise ValueError("La date logique Airflow est requise pour choisir le fichier")

        month = logical_date.strftime("%Y-%m")
        filename = f"yellow_tripdata_{month}.parquet"
        logging.info("Préparation du chargement du mois %s (%s)", month, filename)

        with tempfile.TemporaryDirectory(prefix="nyc-taxi-") as temp_dir:
            local_file = Path(temp_dir) / filename
            download_trip_file(month, local_file)
            logging.info("Fichier téléchargé : %s (%s octets)", filename, local_file.stat().st_size)

            hook = SnowflakeHook(snowflake_conn_id=CONNECTION_ID)
            connection = hook.get_conn()
            cursor = connection.cursor()
            try:
                put_sql = (
                    f"PUT file://{local_file.resolve()} @{RAW_STAGE} "
                    "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
                cursor.execute(put_sql)
                logging.info("Résultat PUT pour %s : %s", filename, cursor.fetchall())

                cursor.execute(
                    f"DELETE FROM {RAW_TABLE} WHERE _source_file = %s",
                    (filename,),
                )
                logging.info(
                    "Anciennes lignes supprimées pour %s : %s",
                    filename,
                    cursor.rowcount,
                )

                cursor.execute(COPY_SQL.format(month=month))
                logging.info("Résultat COPY INTO pour %s : %s", filename, cursor.fetchall())
            finally:
                cursor.close()
                connection.close()

    download_and_load_month()


load_yellow_trips()