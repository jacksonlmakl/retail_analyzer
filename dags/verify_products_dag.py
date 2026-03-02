"""
Airflow DAG: Verify stale product listings on Google Shopping.

Runs daily (configurable). Triggers verification via the Retail Analyzer
API endpoint, making it cloud-portable — Airflow and the API can live on
different providers as long as the API URL is reachable.
"""

import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

API_BASE_URL = os.environ.get("RETAIL_API_URL", "http://api:8000")
STALE_DAYS = int(os.environ.get("VERIFY_STALE_DAYS", "7"))

default_args = {
    "owner": "retail_analyzer",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def trigger_verify(**context):
    """Call the API's /verify endpoint and return the queued task ID."""
    params = context.get("params", {})
    max_stale_days = params.get("max_stale_days", STALE_DAYS)

    url = f"{API_BASE_URL.rstrip('/')}/verify"
    resp = requests.post(url, json={"max_stale_days": max_stale_days}, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    task_id = data["task_id"]
    print(f"[+] Enqueued verify via API: task_id={task_id}")
    return task_id


with DAG(
    dag_id="verify_products",
    default_args=default_args,
    description="Verify stale Google Shopping product listings",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={"max_stale_days": STALE_DAYS},
    tags=["retail_analyzer", "verification"],
) as dag:

    verify_task = PythonOperator(
        task_id="trigger_verify",
        python_callable=trigger_verify,
    )
