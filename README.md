# pet_opensky
pet_opensky



# Настройка Airflow Variables и Connections

После запуска стека (`docker compose up -d`) необходимо вручную создать
Variables и Connections в Airflow UI (http://localhost:8080).

---

## Variables
**Admin → Variables → + (Add)**

| Key           | Value                                    |
|---------------|------------------------------------------|
| NASA_API_KEY  | ключ с api.nasa.gov                  |
| MINIO_BUCKET  | raw-data                                 |
| KEY_PREFIX    | asteroids                                |
| CH_TABLE      | nasa.asteroids                           |
| CH_MIN_ROWS   | 1                                        |

---

## Connections
**Admin → Connections → + (Add)**

---

### minio_s3

| Поле                  | Значение                          |
|-----------------------|-----------------------------------|
| Connection Id         | minio_s3                          |
| Connection Type       | Amazon Web Services               |
| AWS Access Key ID     | minioadmin                        |
| AWS Secret Access Key | minioadmin                        |
| Extra                 | `{"endpoint_url": "http://minio:9000", "bucket_name": "raw-data"}` |

---

### clickhouse_default

| Поле            | Значение           |
|-----------------|--------------------|
| Connection Id   | clickhouse_default |
| Connection Type | Generic            |
| Host            | clickhouse         |
| Port            | 8123               |
| Login           | default            |
| Password        | (пусто)            |
| Schema          | nasa               |

---

### spark_default

| Поле            | Значение     |
|-----------------|--------------|
| Connection Id   | spark_default |
| Connection Type | Spark        |
| Host            | local        |
| Deploy mode     | client       |
| Spark binary    | spark-submit |