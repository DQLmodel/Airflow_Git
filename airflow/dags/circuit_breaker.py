from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from dqlabs.dq_package.operators import DQLabsCircuitBreakerOperator
from airflow.operators.python import ShortCircuitOperator
default_args = {
 'owner': 'airflow',
 'depends_on_past': False,
 'email_on_failure': False,
 'email_on_retry': False
}
with DAG(
 'test_circuit_breaker_example',
 default_args=default_args,
 start_date=datetime(2022, 2, 8),
 catchup=False,
 schedule_interval=None,
 tags=["circuit"]
) as dag:
 task1 = BashOperator(
 task_id='example_elt_job_1',
 bash_command='echo I am transforming a very important table!',
 )
 cond_true = DQLabsCircuitBreakerOperator(
 task_id='condition_is_True',
 rule={'asset_id': '489ef33f-0ef2-444b-8d97-d506e7e68a52',
 'condition': '<', 'threshold': 0}
 )
 cond_false = DQLabsCircuitBreakerOperator(
 task_id='condition_is_False',
 rule={'connection': {'connection_name': 'SNOWFAKE_LINEAGE_CHECK', 'database_name': 'DQLABS', 'schema_name':
'CUSTOMERAI', 'table_name': 'CUSTOMERAI_INCREMENTAL'},
 'condition': '>', 'threshold': 50}
 )
 task2 = BashOperator(
 task_id='example_elt_job_2',
 bash_command='echo I am building a very important dashboard from the table created in task1!',
 trigger_rule='none_failed'
 )
 task3 = BashOperator(
 task_id='example_elt_job_3',
 bash_command='echo I am building a very important dashboard from the table created in task3!',
 trigger_rule='none_failed'
 )
 task1 >> [cond_true, cond_false] >> task2 >> task3
