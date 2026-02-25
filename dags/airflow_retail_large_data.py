from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


default_args = {
    'owner': 'hamidi',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

dag = DAG(
    'retail_etl_pipeline_large_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# 1️⃣ Copy CSV to master DB
def copy_to_master():
    pg_hook = PostgresHook(postgres_conn_id='postgres_artha_master')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SET datestyle TO 'ISO, DMY';")
    cursor.execute("TRUNCATE TABLE retail_master;")

    with open('/opt/airflow/data/retail-dataset.csv', 'r', encoding='latin1') as f:
        cursor.copy_expert(
            """
            COPY retail_master
            FROM STDIN
            WITH (
                FORMAT CSV,
                HEADER TRUE,
                DELIMITER ','
            )
            """,
            f
        )

    conn.commit()
    cursor.close()
    conn.close()


copy_task = PythonOperator(
    task_id='copy_to_master',
    python_callable=copy_to_master,
    dag=dag
)

# 2️⃣ Transform using FDW (warehouse side)
transform_task = PostgresOperator(
    task_id='transform_to_warehouse',
    postgres_conn_id='postgres_artha_warehouse',
    sql=[
        "TRUNCATE TABLE retail_sales;",
        """
        INSERT INTO retail_sales (
            invoiceno,
            stockcode,
            description,
            quantity,
            invoicedate,
            unitprice,
            customerid,
            country,
            totalamount
        )
        SELECT
            invoiceno,
            stockcode,
            COALESCE(description, 'RED RETROSPOT MINI CASES'),
            quantity,
            invoicedate,
            unitprice,
            COALESCE(customerid, 17920),
            country,
            quantity * unitprice
        FROM retail_master
        WHERE invoicedate >= DATE '2010-12-10'
        AND invoicedate <  DATE '2010-12-11';
        """
    ]
)

copy_task >> transform_task