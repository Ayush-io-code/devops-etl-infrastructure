from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extracting data from source...")
    return [{"id": 1, "value": 100}, {"id": 2, "value": 200}]

def transform(**context):
    data = context['ti'].xcom_pull(task_ids='extract')
    transformed = [{"id": r["id"], "value": r["value"] * 1.1} for r in data]
    print(f"Transformed: {transformed}")
    return transformed

def load(**context):
    data = context['ti'].xcom_pull(task_ids='transform')
    print(f"Loading {len(data)} records to destination")

with DAG('etl_pipeline', start_date=datetime(2024,1,1),
         schedule='@daily', catchup=False) as dag:
    t1 = PythonOperator(task_id='extract', python_callable=extract)
    t2 = PythonOperator(task_id='transform', python_callable=transform)
    t3 = PythonOperator(task_id='load', python_callable=load)
    t1 >> t2 >> t3
