# ---------------------------------------------------------------------------
# Stage 1: Загрузчик (Download Stage)
# Используем Alpine для скачивания, так как он легкий
# ---------------------------------------------------------------------------
FROM alpine:latest AS downloader

ENV SPARK_VERSION=3.5.4
RUN mkdir -p /build/spark /build/jars

# Устанавливаем curl для скачивания с индикатором прогресса
RUN apk add --no-cache curl tar 

# Скачиваем Spark с выводом прогресса (-#)
RUN curl -# -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o spark.tgz && \
    tar -xzf spark.tgz -C /build/spark --strip-components=1 && \
    rm spark.tgz

# Скачиваем JAR-файлы (по одному, чтобы видеть прогресс каждого)
RUN curl -# -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -o /build/jars/hadoop-aws-3.3.4.jar && \
    curl -# -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -o /build/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -# -L "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.1/clickhouse-jdbc-0.7.1-all.jar" -o /build/jars/clickhouse-jdbc-0.7.1-all.jar

# ---------------------------------------------------------------------------
# Stage 2: Финальный образ
# ---------------------------------------------------------------------------
FROM apache/airflow:2.10.5-python3.12

USER root

# Системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

# Копируем Spark и JARs из первого этапа
COPY --from=downloader /build/spark /opt/spark
COPY --from=downloader /build/jars /opt/spark/extra-jars

# Настройка прав
RUN chown -R airflow:root /opt/spark

USER airflow

# Python-зависимости
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --default-timeout=600 -r /tmp/requirements.txt