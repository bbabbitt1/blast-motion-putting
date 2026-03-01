from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

sys.path.insert(0, '/opt/airflow/dags/blast-motion-putting')

from main import run_pipeline

default_args = {
    'owner': 'bbabbitt1',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='blast_motion_putting',
    default_args=default_args,
    description='Fetches putting data from Blast Connect and loads to PostgreSQL',
    schedule='0 */2 * * *',
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['blast', 'putting', 'golf']
) as dag:

    fetch_and_load = PythonOperator(
        task_id='fetch_and_load_putts',
        python_callable=run_pipeline,
    )