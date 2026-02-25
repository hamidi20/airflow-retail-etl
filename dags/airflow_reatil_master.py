from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd


default_args = {
    'owner': 'hamidi',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

dag = DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# 1️⃣ Create Table Task
create_table = PostgresOperator(
    task_id='create_retail_master',
    postgres_conn_id='postgres_artha_master',
    sql="""
        CREATE TABLE IF NOT EXISTS retail_master (
            invoiceno VARCHAR(50),
            stockcode VARCHAR(50),
            description TEXT,
            quantity INT,
            invoicedate TIMESTAMP,
            unitprice NUMERIC(10,2),
            customerid BIGINT,
            country VARCHAR(225)
        );
    """,
    dag=dag
)

# 2️⃣ Load CSV Task
def load_csv():
    df = pd.read_csv(
        '/opt/airflow/data/retail-dataset.csv',
        encoding='latin1'
    )
    
    df.columns = df.columns.str.lower()
    df['invoicedate'] = pd.to_datetime(df['invoicedate'], dayfirst=True)

    pg_hook = PostgresHook(postgres_conn_id='postgres_artha_master')
    engine = pg_hook.get_sqlalchemy_engine()

    df.to_sql(
        'retail_master',
        engine,
        if_exists='replace',
        index=False
    )

load_data = PythonOperator(
    task_id='load_csv_to_master',
    python_callable=load_csv,
    dag=dag
)



# 2️⃣ Load to Datawarehouse Task
create_warehouse_table = PostgresOperator(
    task_id='create_retail_warehouse',
    postgres_conn_id='postgres_artha_warehouse',
    sql="""
        CREATE TABLE IF NOT EXISTS retail_sales (
            invoiceno VARCHAR(50),
            stockcode VARCHAR(50),
            description TEXT,
            quantity INT,
            invoicedate TIMESTAMP,
            unitprice NUMERIC(10,2),
            customerid BIGINT,
            country VARCHAR(225),
            totalamount NUMERIC(12,2)
        );
    """,
    dag=dag
)

def transform_to_warehouse():

    master_hook = PostgresHook(postgres_conn_id='postgres_artha_master')
    warehouse_hook = PostgresHook(postgres_conn_id='postgres_artha_warehouse')

    master_conn = master_hook.get_conn()
    warehouse_engine = warehouse_hook.get_sqlalchemy_engine()

    query = """
        SELECT
            invoiceno,
            stockcode,
            COALESCE(description, 'RED RETROSPOT MINI CASES') AS description,
            quantity,
            invoicedate,
            unitprice,
            COALESCE(customerid, 17920) AS customerid,
            country,
            quantity * unitprice AS totalamount
        FROM retail_master
        WHERE DATE_PART('year', invoicedate) = 2010
          AND DATE_PART('month', invoicedate) = 12
          AND DATE_PART('day', invoicedate) = 10
    """

    df = pd.read_sql(query, master_conn)

    df.to_sql(
        'retail_sales',
        warehouse_engine,
        if_exists='replace',
        index=False
    )

    print("Transform & Load to warehouse success:", len(df))
    
transform_task = PythonOperator(
    task_id='transform_to_warehouse',
    python_callable=transform_to_warehouse,
    dag=dag
)

create_table >> load_data >> create_warehouse_table >> transform_task