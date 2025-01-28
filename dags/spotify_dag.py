import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
# sys.path.append("/opt/airflow/scripts")
from datetime import datetime, timedelta
from scripts.etl.fetch_listening_history import fetch_listening_history
from scripts.auth.connect_spotify_api import connect_to_spotify_api


def test_spotify_connection():
    """
    Test the connection to Spotify API.
    """
    sp = connect_to_spotify_api()
    print("Spotify connected successfully!")

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="spotify_workflow_dag",
    default_args=default_args,
    description="A DAG to connect to Spotify API and fetch listening history",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spotify", "api"],
) as dag:

    # Task to test the Spotify connection
    test_connection_task = PythonOperator(
        task_id="test_spotify_connection",
        python_callable=test_spotify_connection,
    )

    # Task to fetch listening history
    fetch_history_task = PythonOperator(
        task_id="fetch_listening_history",
        python_callable=fetch_listening_history,
    )

    # Set the task dependencies
    test_connection_task >> fetch_history_task
