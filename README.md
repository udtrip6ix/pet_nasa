# pet_nasa 

A data engineering pet project that builds a pipeline for loading and analyzing asteroid data from the NASA NeoWs API.

## Architecture

```
NASA API → Airflow DAG 1 → MinIO (Parquet) → Airflow DAG 2 → Spark → ClickHouse → Metabase
```

**Stack:**
- **Airflow** (CeleryExecutor) — pipeline orchestration
- **MinIO** — S3-compatible storage for raw Parquet files
- **Apache Spark** — data transformation and loading
- **ClickHouse** — analytical data warehouse
- **Metabase** — visualization and dashboards
- **PostgreSQL** — metadata store for Airflow and Metabase
- **Redis** — Celery message broker

---

## Quick Start

### 1. Clone the repository

```bash
git clone <repo_url>
cd pet_nasa
```

### 2. Configure `.env`

Copy the template and fill in your values:

```bash
cp .env.example .env
```

Minimal `.env` to get started:

```env
# Airflow
# Generate Fernet key: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW_UID=1000        # find yours with: id -u
AIRFLOW_PROJ_DIR=.
AIRFLOW_FERNET_KEY=your-fernet-key-here
AIRFLOW_ADMIN_USERNAME=airflow
AIRFLOW_ADMIN_PASSWORD=airflow

# PostgreSQL
AIRFLOW_POSTGRES_USER=airflow
AIRFLOW_POSTGRES_PASSWORD=airflow
AIRFLOW_POSTGRES_DB=airflow

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# ClickHouse
CH_HOST=clickhouse
CH_PORT=8123
CH_DATABASE=nasa
CH_USER=default
CH_PASSWORD=

# NASA
NASA_API_KEY=your-nasa-api-key  # get yours at api.nasa.gov
```

### 3. First run (builds the image)

```bash
docker compose up --build -d
```

> The first build takes a few minutes — Spark and JAR files are downloaded during the build.

### 4. Configure Airflow Variables and Connections

Once the stack is up, open Airflow UI at **http://localhost:8080**

#### Variables
**Admin → Variables → + (Add)**

| Key          | Value               |
|--------------|---------------------|
| NASA_API_KEY | key from api.nasa.gov |
| MINIO_BUCKET | raw-data            |
| KEY_PREFIX   | asteroids           |
| CH_TABLE     | nasa.asteroids      |
| CH_MIN_ROWS  | 1                   |
| access_key   | minioadmin          |
| secret_key   | minioadmin          |

#### Connections
**Admin → Connections → + (Add)**

**minio_s3**

| Field                 | Value                                                               |
|-----------------------|---------------------------------------------------------------------|
| Connection Id         | minio_s3                                                            |
| Connection Type       | Amazon Web Services                                                 |
| AWS Access Key ID     | minioadmin                                                          |
| AWS Secret Access Key | minioadmin                                                          |
| Extra                 | `{"endpoint_url": "http://minio:9000", "bucket_name": "raw-data"}` |

**clickhouse_default**

| Field           | Value              |
|-----------------|--------------------|
| Connection Id   | clickhouse_default |
| Connection Type | Generic            |
| Host            | clickhouse         |
| Port            | 8123               |
| Login           | default            |
| Password        | (empty)            |
| Schema          | nasa               |

**spark_default**

| Field           | Value         |
|-----------------|---------------|
| Connection Id   | spark_default |
| Connection Type | Spark         |
| Host            | local         |
| Deploy mode     | client        |
| Spark binary    | spark-submit  |

### 5. Enable DAGs

In the Airflow UI, enable both DAGs:
- `dag_nasa_to_s3` — fetches data from NASA API and stores it in MinIO as Parquet
- `dag_s3_to_ch` — reads Parquet from MinIO and loads it into ClickHouse via Spark

---

## Stack Management

```bash
# Start (after the first build)
docker compose up -d

# Stop (data is preserved)
docker compose down

# Full reset (removes all data)
docker compose down -v

# View logs for a specific service
docker compose logs -f airflow-scheduler
```

---

## Services

| Service        | URL                        | Credentials                        |
|----------------|----------------------------|------------------------------------|
| Airflow UI     | http://localhost:8080      | airflow / airflow                  |
| MinIO Console  | http://localhost:9001      | minioadmin / minioadmin            |
| ClickHouse     | http://localhost:8123/play | default / (empty)                  |
| Metabase       | http://localhost:3000      | configured on first login          |