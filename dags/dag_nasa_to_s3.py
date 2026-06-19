import logging
import requests
import pendulum
import duckdb
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "data-team"
DAG_ID = "dag_nasa_to_s3"
LAYER = "raw-data"
SOURCE = "asteroids"

NASA_API_KEY = Variable.get("NASA_API_KEY", default_var="DEMO_KEY")
MINIO_BUCKET = Variable.get("MINIO_BUCKET", default_var="raw-data")
KEY_PREFIX = Variable.get("KEY_PREFIX", default_var="asteroids")
NASA_FEED_URL = "https://api.nasa.gov/neo/rest/v1/feed"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 19, tz="UTC"),
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

def get_and_transfer_nasa_to_s3(**context):
    ds = context["data_interval_start"]
    date_str = ds.format("YYYY-MM-DD")
    
    logging.info(f"Start load NASA NeoWs data for {date_str}")

    resp = requests.get(
        NASA_FEED_URL,
        params={
            "start_date": date_str,
            "end_date": date_str,
            "api_key": NASA_API_KEY,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    asteroids = data.get("near_earth_objects", {}).get(date_str, [])

    if not asteroids:
        logging.warning(f"0 астероидов за {date_str} — пропускаем")
        return


    rows = []
    for neo in asteroids:
        diameter = neo.get("estimated_diameter", {}).get("kilometers", {})
        approach = neo.get("close_approach_data", [{}])[0]
        rows.append({
            "neo_id": neo["id"],
            "name": neo["name"],
            "diameter_min_km": float(diameter.get("estimated_diameter_min", 0)),
            "diameter_max_km": float(diameter.get("estimated_diameter_max", 0)),
            "is_potentially_hazardous": int(neo.get("is_potentially_hazardous_asteroid", False)),
            "close_approach_date": approach.get("close_approach_date", date_str),
            "relative_velocity_kmh": float(approach.get("relative_velocity", {}).get("kilometers_per_hour", 0)),
            "miss_distance_km": float(approach.get("miss_distance", {}).get("kilometers", 0)),
            "orbiting_body": approach.get("orbiting_body", "Earth"),
        })

    df = pd.DataFrame(rows)
    logging.info(f"Распарсено: {len(df)} астероидов")

    con = duckdb.connect(database=":memory:")
    con.sql(f"""
        INSTALL httpfs; LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_use_ssl = FALSE;
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
    """)


    con.register('asteroids_df', df)

    date_part = ds.format("YYYY/MM/DD")
    file_name = f"{date_str}.parquet"
    s3_path = f"s3://{MINIO_BUCKET}/{KEY_PREFIX}/{date_part}/{file_name}"

    logging.info(f"Saving data to {s3_path}")

    con.sql(f"""
        COPY (
            SELECT 
                neo_id::VARCHAR AS neo_id,
                name::VARCHAR AS name,
                diameter_min_km::DOUBLE AS diameter_min_km,
                diameter_max_km::DOUBLE AS diameter_max_km,
                is_potentially_hazardous::INT8 AS is_potentially_hazardous,
                close_approach_date::VARCHAR AS close_approach_date,
                relative_velocity_kmh::DOUBLE AS relative_velocity_kmh,
                miss_distance_km::DOUBLE AS miss_distance_km,
                orbiting_body::VARCHAR AS orbiting_body
            FROM asteroids_df
        ) TO '{s3_path}' (FORMAT 'PARQUET', COMPRESSION 'SNAPPY', OVERWRITE TRUE);
    """)

    con.close()
    logging.info("Load to S3 finished successfully!")

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 5 * * *",
    catchup=True,
    max_active_runs=3,
    tags=[LAYER, SOURCE]
) as dag:

    start = EmptyOperator(task_id="start")

    extract_and_load = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=get_and_transfer_nasa_to_s3,
    )
    
    end = EmptyOperator(task_id="end")

    start >> extract_and_load >> end