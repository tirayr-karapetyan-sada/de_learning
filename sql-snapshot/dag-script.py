from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Configuration parameter
LOOKBACK_DAYS = 7
# Path to your SQL file
SQL_FILE_PATH = ''

default_args = {
    'owner': '',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='daily_snapshot_refresh',
    default_args=default_args,
    description='Daily refresh of snapshot table',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bigquery', 'snapshot'],
) as dag:

    start = EmptyOperator(task_id='start')

    with open(SQL_FILE_PATH, 'r') as f:
        sql_query = f.read()

    refresh_snapshot = BigQueryInsertJobOperator(
        task_id='refresh_snapshot',
        configuration={
            "query": {
                "query": sql_query,
                "queryParameters": [
                    {
                        "name": "start_date",
                        "parameterType": {"type": "DATE"},
                        "parameterValue": {
                            "value": "{{ (execution_date - macros.timedelta(days=" + str(LOOKBACK_DAYS) + ")).strftime('%Y-%m-%d') }}"
                        }
                    },
                    {
                        "name": "end_date",
                        "parameterType": {"type": "DATE"},
                        "parameterValue": {
                            "value": "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
                        }
                    }
                ],
                "useLegacySql": False,
            }
        },
        location='US',
    )
    end = EmptyOperator(task_id='end')

    start >> refresh_snapshot >> end
