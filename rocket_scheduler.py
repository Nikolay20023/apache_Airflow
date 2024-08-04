import json
import pathlib

import airflow
import airflow.utils
import airflow.utils.dates
import requests
from requests.exceptions import RequestException
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="dowmload_rocker_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="""curl -o /tmp/launches.json -L
        'https://ll.thespacedevs.com/2.2.0/launch/upcoming/' """,
    dag=dag
)


def _get_pictures():
    pathlib.Path("/tmp/launces")