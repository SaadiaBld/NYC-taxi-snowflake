from __future__ import annotations

import logging
import tempfile
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.sdk import dag, get_current_context, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator


CONNECTION_ID = "snowflake_nyc_taxi"
RAW_TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
RAW_STAGE = "NYC_TAXI.RAW.TAXI_STAGE"
PARQUET_FORMAT = "NYC_TAXI.RAW.PARQUET_FORMAT"
TRIPDATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
SQL_DIRECTORY = Path(__file__).resolve().parents[1] / "include" / "sql"

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
    dag_id="nyc_taxi_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=1)},
    template_searchpath=[str(SQL_DIRECTORY)],
    params={
        "max_trip_distance_miles": 100,
        "max_trip_duration_min": 180,
        "max_rejection_pct": 30,
        "start_month": "2025-01-01",
        "end_month": "2025-04-01",
    },
    tags=["snowflake", "raw", "monthly"],
    doc_md=(
        "Charge le fichier TLC Yellow Taxi du mois correspondant à la date logique, "
        "puis transforme et contrôle les données jusqu'aux marts."
    ),
)
def nyc_taxi_pipeline():
    def execute_sql(task_id: str, sql_file: str) -> SQLExecuteQueryOperator:
        return SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id=CONNECTION_ID,
            sql=sql_file,
            split_statements=True,
        )

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

    load_raw = download_and_load_month()

    check_raw_month = SQLCheckOperator(
        task_id="check_raw_month_loaded",
        conn_id=CONNECTION_ID,
        sql="controles/raw_mois_charge.sql",
        retries=0,
    )

    with TaskGroup(group_id="staging", tooltip="Vues et tables de référence") as staging:
        execute_sql("codes_tlc", "staging/codes_tlc.sql")
        execute_sql("stg_taxi_zones", "staging/stg_tlc__taxi_zones.sql")
        execute_sql("stg_yellow_trips", "staging/stg_tlc__yellow_trips.sql")

    create_intermediate_tables = execute_sql("create_intermediate_tables", "00_tables.sql")

    with TaskGroup(
        group_id="intermediate", tooltip="Contrôle et enrichissement des trajets"
    ) as intermediate:
        flagged = execute_sql("flag_trips", "intermediate/int_trips__flagged.sql")
        check_rejection_rate = SQLCheckOperator(
            task_id="check_rejection_rate",
            conn_id=CONNECTION_ID,
            sql="controles/taux_trajets_rejetes.sql",
            retries=0,
        )
        enriched = execute_sql("enrich_trips", "intermediate/int_trips__enriched.sql")
        check_duplicate_trips = SQLCheckOperator(
            task_id="check_duplicate_trips",
            conn_id=CONNECTION_ID,
            sql="controles/trajets_en_double.sql",
            retries=0,
        )
        flagged >> check_rejection_rate >> enriched >> check_duplicate_trips

    with TaskGroup(group_id="marts", tooltip="Dimensions, faits et tables d'analyse") as marts:
        dim_date = execute_sql("dim_date", "marts/dim_date.sql")
        dim_payment_type = execute_sql("dim_payment_type", "marts/dim_payment_type.sql")
        dim_rate_code = execute_sql("dim_rate_code", "marts/dim_rate_code.sql")
        dim_vendor = execute_sql("dim_vendor", "marts/dim_vendor.sql")
        dim_zone = execute_sql("dim_zone", "marts/dim_zone.sql")
        fact_trips = execute_sql("fact_trips", "marts/fct_trips.sql")
        daily_revenue = execute_sql("mart_daily_revenue", "marts/mart_daily_revenue.sql")
        execute_sql("mart_data_quality", "marts/mart_data_quality.sql")
        zone_hourly_demand = execute_sql(
            "mart_zone_hourly_demand", "marts/mart_zone_hourly_demand.sql"
        )

        [fact_trips, dim_date, dim_payment_type] >> daily_revenue
        [fact_trips, dim_zone] >> zone_hourly_demand

    load_raw >> check_raw_month
    check_raw_month >> [staging, create_intermediate_tables]
    [staging, create_intermediate_tables] >> intermediate
    intermediate >> marts


dag = nyc_taxi_pipeline()