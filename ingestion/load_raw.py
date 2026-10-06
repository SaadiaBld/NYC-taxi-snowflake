import os
import sys
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def required_setting(name):
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Erreur : variable {name} manquante dans .env")
    return value


ACCOUNT = required_setting("SNOWFLAKE_ACCOUNT")
USER = required_setting("SNOWFLAKE_USER")
ROLE = required_setting("SNOWFLAKE_ROLE")
WAREHOUSE = required_setting("SNOWFLAKE_WAREHOUSE")
DATABASE = required_setting("SNOWFLAKE_DATABASE")
SCHEMA = required_setting("SNOWFLAKE_SCHEMA")

PRIVATE_KEY_PATH = Path(
    os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "airflow_rsa_key.p8")
)
if not PRIVATE_KEY_PATH.is_absolute():
    PRIVATE_KEY_PATH = PROJECT_ROOT / PRIVATE_KEY_PATH
DATA_DIR = PROJECT_ROOT / "data/raw"


# ---------------------------------------------------------
# Validation de l'argument
# ---------------------------------------------------------

if len(sys.argv) != 2:
    print("Usage : python3 ingestion/load_raw.py YYYY-MM")
    sys.exit(1)

month = sys.argv[1]

if len(month) != 7 or month[4] != "-":
    print("Erreur : le mois doit être au format YYYY-MM")
    sys.exit(1)


parquet_file = DATA_DIR / f"yellow_tripdata_{month}.parquet"

if not parquet_file.exists():
    print(f"Erreur : fichier introuvable : {parquet_file}")
    sys.exit(1)


# ---------------------------------------------------------
# Lecture de la clé privée
# ---------------------------------------------------------

with open(PRIVATE_KEY_PATH, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)


# ---------------------------------------------------------
# Connexion Snowflake
# ---------------------------------------------------------

conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    role=ROLE,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
    private_key=private_key_bytes,
)


try:
    cursor = conn.cursor()

    # -----------------------------------------------------
    # PUT : fichier local -> stage
    # -----------------------------------------------------

    put_sql = f"""
        PUT file://{parquet_file.resolve()}
        @TAXI_STAGE
        AUTO_COMPRESS=FALSE
        OVERWRITE=FALSE
    """

    print(f"PUT : {parquet_file}")

    cursor.execute(put_sql)

    for row in cursor.fetchall():
        print(row)

    # -----------------------------------------------------
    # COPY INTO : stage -> RAW
    # -----------------------------------------------------

    copy_sql = f"""
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
                $1:airport_fee::DECIMAL,
                $1:cbd_congestion_fee::DECIMAL,
                METADATA$FILENAME,
                CURRENT_TIMESTAMP()
            FROM @TAXI_STAGE/yellow_tripdata_{month}.parquet
                (FILE_FORMAT => PARQUET_FORMAT)
        )
    """

    print(f"COPY INTO : {month}")

    cursor.execute(copy_sql)

    for row in cursor.fetchall():
        print(row)

finally:
    cursor.close()
    conn.close()