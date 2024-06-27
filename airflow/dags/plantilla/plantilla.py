from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "nombre",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 26), # dependiendo la fecha de ejecucion
    "email": ["correo@tecZitacuaro.com"]
}
params = {
    "env": "dev" #beta prod test
}

with DAG(
    dag_id="plantilla",
    default_args=default_args,
    schedule="@once", # */5 * * * * ... crontab
    max_active_tasks=1,
    max_active_runs=1,
    catchup=False,
    tags=["curso_tec"],
    params=params
) as dag:
    
    def funcion_uno():
        print("hola mundo")

    start = DummyOperator(
        task_id="start"
    )
    hola_mundo = PythonOperator(
        task_id="hola_mundo",
        python_callable=funcion_uno
    )
    end = DummyOperator(
        task_id="end"
    )

    start >> hola_mundo >> end
