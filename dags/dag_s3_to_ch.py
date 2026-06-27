from __future__ import annotations

import logging

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

log = logging.getLogger(__name__)

SPARK_JOB_PATH = "/opt/airflow/spark/spark_job.py"
SPARK_JARS = ",".join([
    "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar",
    "/opt/spark/extra-jars/clickhouse-jdbc-0.7.1-all.jar",
])

MINIO_BUCKET = Variable.get("MINIO_BUCKET", default_var="raw-data")
KEY_PREFIX   = Variable.get("KEY_PREFIX",   default_var="asteroids")
CH_TABLE     = Variable.get("CH_TABLE",     default_var="nasa.asteroids")
CH_MIN_ROWS  = int(Variable.get("CH_MIN_ROWS", default_var="1"))

DEFAULT_ARGS={
    "owner":"ud6",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "email_on_failure" : False

}

def prepare_spark_env(**context):
    """
    Собирает только параметры запуска (не секреты) и пушит в XCom.
    Credentials Spark читает сам из env-переменных контейнера.
    """
    logical_date = context["logical_date"]
    date_part    = logical_date.strftime("%Y/%m/%d")
    date_str     = logical_date.strftime("%Y-%m-%d")

    s3_path = f"s3a://{MINIO_BUCKET}/{KEY_PREFIX}/{date_part}/{date_str}.parquet"

    context["ti"].xcom_push(key="s3_path", value=s3_path)
    log.info("s3_path=%s", s3_path)


def verify_load(**context):
    """
    Проверяет что строки за нужный день появились в ClickHouse.
    Credentials читает из env-переменных контейнера напрямую.
    """
    import os
    import clickhouse_connect

    logical_date = context["logical_date"]
    date_str     = logical_date.strftime("%Y-%m-%d")

    client = clickhouse_connect.get_client(
        host=os.environ["CH_HOST"],
        port=int(os.environ.get("CH_PORT", "8123")),
        database=os.environ.get("CH_DATABASE", "nasa"),
        username=os.environ.get("CH_USER", "default"),
        password=os.environ.get("CH_PASSWORD", ""),
    )

    result = client.query(f"""
        SELECT count() AS cnt
        FROM {CH_TABLE} FINAL
        WHERE close_approach_date = '{date_str}'
    """)

    row_count = result.first_row[0]
    log.info("Строк в CH за %s: %d (минимум: %d)", date_str, row_count, CH_MIN_ROWS)

    if row_count < CH_MIN_ROWS:
        raise ValueError(
            f"Verification failed: {CH_TABLE} за {date_str} "
            f"содержит {row_count} строк, ожидалось ≥ {CH_MIN_ROWS}"
        )

    log.info("Verification OK ✓")

with DAG(
    dag_id="dag_s3_to_ch",
    default_args=DEFAULT_ARGS,
    description="ExternalTaskSensor → SparkSubmit → ClickHouse",
    schedule_interval="0 5 * * *", 
    start_date=pendulum.datetime(2026, 5, 19, tz="UTC"),
    catchup=True,
    max_active_runs=3,
    tags=["nasa", "spark", "clickhouse"],
) as dag:

    start = EmptyOperator(task_id="start")

    t_sensor = ExternalTaskSensor(
        task_id="wait_for_dag_nasa_to_s3",
        external_dag_id="dag_nasa_to_s3",
        external_task_id=None,           # ждём весь DagRun, не конкретную таску
        execution_date_fn=lambda dt: dt, # тот же logical_date
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",               # освобождает воркер между проверками
        poke_interval=30,
        timeout=60 * 60 * 2,
        check_existence=True,
    )

    t_prepare = PythonOperator(
        task_id="prepare_spark_env",
        python_callable=prepare_spark_env,
    )

    t_spark = SparkSubmitOperator(
        task_id="spark_s3_to_ch",
        conn_id="spark_default",
        application=SPARK_JOB_PATH,
        jars=SPARK_JARS,
        application_args=[
            "--s3-path",  "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='s3_path') }}",
            "--ch-table", CH_TABLE,
        ],
        execution_timeout=pendulum.duration(hours=1),
        verbose=True,
    )

    t_verify = PythonOperator(
        task_id="verify_clickhouse_load",
        python_callable=verify_load,
    )

    end = EmptyOperator(task_id="end")

    start >> t_sensor >> t_prepare >> t_spark >> t_verify >> end