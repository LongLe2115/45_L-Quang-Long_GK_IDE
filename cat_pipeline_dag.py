from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.crawl import crawl_cat_data
from src.transform import transform_cat_data
from src.save import create_table, save_data, download_images
import psycopg2

def task_crawl(ti):
    raw = crawl_cat_data()
    ti.xcom_push(key="raw_data", value=raw)

def task_transform(ti):
    raw = ti.xcom_pull(key="raw_data", task_ids="crawl_cat")
    transformed = transform_cat_data(raw)
    ti.xcom_push(key="clean_data", value=transformed)

def task_save(ti):
    cats = ti.xcom_pull(key="clean_data", task_ids="transform_cat")

    # ⚠ Nếu chạy DAG trong container → host="postgres"
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",   # ← Airflow container dùng host này
        port="5432"
    )

    create_table(conn)
    save_data(conn, cats)
    download_images(cats)

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="cat_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 9 * * *",  # Chạy 9h sáng mỗi ngày
    catchup=False,
    description="Pipeline crawl - transform - save dữ liệu mèo"
) as dag:

    t1 = PythonOperator(
        task_id="crawl_cat",
        python_callable=task_crawl
    )

    t2 = PythonOperator(
        task_id="transform_cat",
        python_callable=task_transform
    )

    t3 = PythonOperator(
        task_id="save_cat",
        python_callable=task_save
    )

    t1 >> t2 >> t3
