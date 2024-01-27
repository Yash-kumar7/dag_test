# test_integration.py

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
import main_dag

def test_integration():
    dag = DAG(
        'test_integration',
        default_args={
            'owner': 'airflow',
            'start_date': days_ago(1),
        },
        schedule_interval=None,
    )

    with dag:
        t1 = PythonOperator(
            task_id='fetch_app_usage_logs',
            python_callable=fetch_app_usage_logs,
        )

        t2 = PythonOperator(
            task_id='segregate_logs',
            python_callable=segregate_logs,
        )

        t3 = PythonOperator(
            task_id='extract_booking_logs',
            python_callable=extract_booking_logs,
        )

        t4 = PythonOperator(
            task_id='transfer_to_snowflake',
            python_callable=transfer_to_snowflake,
        )

        t5 = PythonOperator(
            task_id='acquire_occupancy_logs',
            python_callable=acquire_occupancy_logs,
        )

        t6 = PythonOperator(
            task_id='transform_logs',
            python_callable=transform_logs,
        )

    # Define the DAG's task dependencies
    t1 >> t2 >> t4
    t3 >> t4
    t5 >> t6

    # Trigger the DAG and check the state of the final task
    dag.run(start_date=days_ago(1), end_date=days_ago(0), ignore_ti_state=True)

    ti = TaskInstance(task=t6, execution_date=days_ago(1))
    ti.refresh_from_db()

    # Check if the final task succeeded
    assert ti.state == 'success'
