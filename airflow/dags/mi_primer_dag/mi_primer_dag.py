from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
 
mis_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False
}
 
with DAG(
    dag_id="mi_primer_dag",
    default_args=mis_args,
    start_date=datetime(2024, 6, 26),
    schedule="@once", # * * * * * ... crontab
    catchup=False,
    tags=["hola_mundo", "si_funciona", "curso_tec"],
) as dag:
    start = DummyOperator(
        task_id="start"
    )
    tarea = BashOperator(
        task_id="hola_mundo",
        bash_command="echo 'hola mundo'",
    )
    end = DummyOperator(
        task_id="end"
    )
 
    start >> tarea >> end