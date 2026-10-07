import pendulum

from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test", "snowflake"],
)
def test_snowflake_connection():

    test_connection = SQLExecuteQueryOperator(
        task_id="test_connection",
        conn_id="snowflake_nyc_taxi",
        sql="""
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE();
        """,
    )


test_snowflake_connection()