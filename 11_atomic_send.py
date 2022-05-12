import datetime as dt
from importlib.resources import path
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG (
    dag_id="05_query_with_dates",
    schedule_interval=dt.timedelta(days=3),
    start_date=dt.datetime(year=2021, month=1, day=1),
    end_date=dt.datetime(year=2021, month=1, day=5),
#09_no_catchup
    catchup=False,
    
)

fetch_events = BashOperator (
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data &&"
        "curl -o /data/events.json "
        "http://localhost:5000/events"
        "start_date={{ds}}&"
        "end_date={{next_ds}}"
    ),
    dag=dag,
)

def _calculate_stats(**context) :
    """Calculates event statistics."""
#08_templated_path
    input_path = context["templates_dict"] ["input_path"]
    output_path = context["templates_dict"] ["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", 'user']).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

#11_atomic_send
def _send_stats(email, **context) :
    stats= pd.read_csv(context["templates_dict"] ["stats_path"])
    email_stats(stats, email=email)

    _send_stats = PythonOperator(
        task_id="send_stats",
        python_callable=_send_stats,
        op_kwargs={"email": "daniil.morzhevskiy@gmail.com"}
        templates_dict={"stats_path": "/data/stats/{{ds}}.csv"},
        dag=dag,
    )

calculate_stats >> send_stats

#10_non_atomic_send
#    email_state(stats, email="daniil.morzhevskiy@gmail.com")

    calculate_stats = PythonOperator (
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        op_kwargs={
#08_templated_path
            "input_path": "/data/events/{{ds}}.json",
            "output_path": "/data/stats//{{ds}}.csv",
        },
        dag=dag,
    )

    fetch_events >> calculate_stats