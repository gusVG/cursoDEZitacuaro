from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime

default_args = {
    "owner": "Fulano",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 1),
    "email": ["correo@tecZitacuaro.com"]
}
params = {
    "env": "dev"
}

with DAG(
    dag_id="cross_com",
    default_args=default_args,
    schedule="*/2 * * * *",
    max_active_tasks=1,
    max_active_runs=1,
    catchup=False,
    tags=["curso_tec"],
    params=params
) as dag:
    
    def suma():
        ctx = get_current_context()
        x = ctx.get("x")
        y = ctx.get("y")
        return x + y # xcom # push
    
    def mostrar():
        ctx = get_current_context()
        var_pull = ctx.get("var_pull")
        print(var_pull)
        print("---------------------")
        ti = ctx.get("ti")
        print(ti.xcom_pull(task_ids='xcom_push'))
        print("---------------------")
        print(ctx.get("data_interval_start"))
        print(ctx.get("data_interval_end"))
        print(ctx.get("ds"))
        print(ctx.get("dag"))

    start = DummyOperator(
        task_id="start"
    )
    xcom_push = PythonOperator(
        task_id="xcom_push",
        python_callable=suma,
        op_kwargs={
            "x": 5,
            "y": 6,
        }
    )
    xcom_pull = PythonOperator(
        task_id="xcom_pull",
        python_callable=mostrar,
        op_kwargs={
            "var_pull": "{{ti.xcom_pull(task_ids='xcom_push')}}",
        }
    )
    end = DummyOperator(
        task_id="end"
    )

    start >> xcom_push >> xcom_pull >> end
