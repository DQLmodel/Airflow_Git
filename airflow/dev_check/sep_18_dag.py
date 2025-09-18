from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dbt_product_models',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt']
) as dag:

    run_model_customer = SSHOperator(
        task_id='customer',
        ssh_conn_id='ssh_dbt_server',
        command="""
            cd /home/ubuntu/dbt-core/ &&
            source dbt-env/bin/activate &&
            cd snowflake_project &&
            dbt run --select employee_sales_vw
        """,
        do_xcom_push=False
    )

    run_model_product = SSHOperator(
        task_id='product',
        ssh_conn_id='ssh_dbt_server',
        command="""
            cd /home/ubuntu/dbt-core/ &&
            source dbt-env/bin/activate &&
            cd snowflake_project &&
            dbt run --select product_sales_vw
        """,
        do_xcom_push=False
    )

    run_model_sales = SSHOperator(
        task_id='sales',
        ssh_conn_id='ssh_dbt_server',
        command="""
            cd /home/ubuntu/dbt-core/ &&
            source dbt-env/bin/activate &&
            cd snowflake_project &&
            dbt run --select total_sales_vw
        """,
        do_xcom_push=False
    )
    
    run_model_all = SSHOperator(
        task_id='overall',
        ssh_conn_id='ssh_dbt_server',
        command="""
            cd /home/ubuntu/dbt-core/ &&
            source dbt-env/bin/activate &&
            cd snowflake_project &&
            dbt run --select product_models
        """,
        do_xcom_push=False    
    )

    run_model_customer >> run_model_product >> run_model_sales >> run_model_all
