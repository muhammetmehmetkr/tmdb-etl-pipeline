from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(dag_id="etl_pipeline", start_date=datetime(2026, 7, 7),
        catchup=False, schedule="@daily") as dag:

    load_credits=SSHOperator(
        task_id="upload_01",
        ssh_conn_id="spark_client",
        command="""
            cd /dataops &&
            python 01_dataframe_to_s3.py \
            -k credits/credits_part \
            -i /dataops/raw_data/tmdb_5000_credits.csv \
            -ofp True \
            -z 500 \
            -b 0.005
        """,
        cmd_timeout=300
    )

    load_movies=SSHOperator(
        task_id="upload_02",
        ssh_conn_id="spark_client",
        command="""
            cd /dataops &&
            python 01_dataframe_to_s3.py \
            -k movies/movies_part \
            -i /dataops/raw_data/tmdb_5000_movies.csv \
            -ofp True \
            -z 500 -b 0.005
        """,
        cmd_timeout=300
    )

    cleansing_credits=SSHOperator(
        task_id="transform_01",
        ssh_conn_id="spark_client",
        command="""
            cd /dataops &&
            python 02_credits_data.py
        """,
        cmd_timeout=300
    )

    cleansing_movies=SSHOperator(
        task_id="transform_02",
        ssh_conn_id="spark_client",
        command="""
            cd /dataops &&
            python 03_movies_data.py
        """,
        cmd_timeout=300
    )

    write_postgres=SSHOperator(
        task_id="writing_db",
        ssh_conn_id="spark_client",
        command="""
            cd /dataops &&
            python 04_load_to_postgres.py
        """,
        cmd_timeout=300
    )

    load_credits >> cleansing_credits
    load_movies >> cleansing_movies
    
    [cleansing_credits, cleansing_movies] >> write_postgres