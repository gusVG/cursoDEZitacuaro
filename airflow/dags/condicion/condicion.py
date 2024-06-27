from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.python import get_current_context
from datetime import datetime

default_args = {
    "owner": "Zeki",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 1),
    "email": ["correo@tecZitacuaro.com"]
}
params = {
    "env": "dev"
}

with DAG(
    dag_id="condicion",
    default_args=default_args,
    schedule=None, # "*/2 * * * *",
    max_active_tasks=1,
    max_active_runs=1,
    catchup=False,
    tags=["curso_tec"],
    params=params
) as dag:
    
    def if_lista_mayor_zero():
        context = get_current_context()
        arreglo = context.get('lista')
        if len(arreglo) > 0:
            return context.get('task_if_true')
        else:
            return context.get('task_if_false')
    
    def imprime():
        context = get_current_context()
        texto = context.get('texto')
        print(texto)


    start = DummyOperator(
        task_id="start"
    )
    if_arreglo_1 = BranchPythonOperator(
        task_id="condicion_arreglo_1",
        python_callable=if_lista_mayor_zero,
        op_kwargs={
            "lista": [1,2,3],
            "task_if_true": "imprime_args_1",
            "task_if_false": "end",
        },
    )
    imprime_1 = PythonOperator(
        task_id="imprime_args_1",
        python_callable=imprime,
        op_kwargs={
            "texto": "imprimiendo 1",
        },
    )
    if_arreglo_2 = BranchPythonOperator(
        task_id="condicion_arreglo_2",
        python_callable=if_lista_mayor_zero,
        op_kwargs={
            "lista": [],
            "task_if_true": "imprime_args_2",
            "task_if_false": "end",
        },
    )
    imprime_2 = PythonOperator(
        task_id="imprime_args_2",
        python_callable=imprime,
        op_kwargs={
            "texto": "imprimiendo 2",
        },
    )
    end = DummyOperator(
        task_id="end", trigger_rule="none_failed"
    )

    start >> [if_arreglo_1, if_arreglo_2]
    if_arreglo_1 >> [imprime_1, end]
    if_arreglo_2 >> [imprime_2, end]
    imprime_1 >> end
    imprime_2 >> end
