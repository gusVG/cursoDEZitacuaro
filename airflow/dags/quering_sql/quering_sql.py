import json
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime
 
default_args = {
    "owner": "nombre",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 1),
    "email": ["nombre@zitacuaro.tecnm.mx"]
}
params = {
    "env": "dev"
}
 
with DAG(
    dag_id="quering_sql",
    default_args=default_args,
    schedule=None,
    max_active_tasks=1,
    max_active_runs=1,
    catchup=False,
    tags=["curso_tec"],
    params=params
) as dag:
   
    def mostrar():
        ctx=get_current_context()
        ti = ctx.get("ti")
        lista = ti.xcom_pull(task_ids='query_init')
        print(lista)
        print(type(lista))
        print(len(lista))
 
    start = DummyOperator(
        task_id="start"
    )
    query_init = SQLExecuteQueryOperator(
        task_id="query_init",
        conn_id="pg_rds_curso",
        sql="sql/query_jinja_template.sql",
        params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
    )
 
    query_pull = PythonOperator(
        task_id="query_pull",
        python_callable=mostrar
    )
 
 
    end = DummyOperator(
        task_id="end"
    )
 
    start >> query_init >> query_pull >> end