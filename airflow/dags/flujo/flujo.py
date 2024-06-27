from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "Ezequiel",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 27),
    "email": ["correo@tecZitacuaro.com"]
}

params = {
    "env": "dev"
}

with DAG(
    dag_id="flujo",
    default_args=default_args,
    schedule=None,
    max_active_tasks=1,
    max_active_runs=1,
    catchup=False,
    tags=["curso_tec"],
    params=params
) as dag:
    
    def hola():
        print("hola")
    
    def mundo():
        print("mundo")

    def imprime():
        print("imprime")

    start = DummyOperator(
        task_id="start"
    )
    hola_op = PythonOperator(
        task_id="hola",
        python_callable=hola
    )
    mundo_op = PythonOperator(
        task_id="mundo",
        python_callable=mundo
    )
    imprime_op = PythonOperator(
        task_id="imprime",
        python_callable=imprime
    )
    end = DummyOperator(
        task_id="end"
    )

    start >> [hola_op, mundo_op] >> imprime_op >> end
