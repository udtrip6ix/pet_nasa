CREATE DATABASE metabase;
ALTER DATABASE metabase OWNER TO airflow;
 
\c metabase
GRANT ALL ON SCHEMA public TO airflow;