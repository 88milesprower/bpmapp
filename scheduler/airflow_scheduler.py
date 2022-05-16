from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta

DBT_PROFILE_DIR = "/home/nryabykh/.dbt"
DBT_PROJECT_DIR = "../dbt"


with DAG(
    'dbt_dag',
    start_date=datetime(2022, 5, 15),
    description='An Airflow DAG to invoke simple dbt commands',
    schedule_interval=timedelta(days=1),
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'dbt run --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'dbt test --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}'
    )

    dbt_run >> dbt_test
