from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta,datetime
from airflow.operators.dummy_operator import DummyOperator
default_args = {
    'owner':'anand_21',
    'depends_on_past':False,
    'start_date':datetime(2022,3,1),
    'email':['divya2103anand@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

# define DAG

dag = DAG(
    '3_DummyOperator',
    default_args=default_args,
    description='How to use Dummy Operator?',
    schedule_interval= timedelta(days=1)
)

start = DummyOperator(
    task_id = 'start',
    dag = dag
)

end = DummyOperator(
    task_id = 'end',
    dag = dag
)

callf1 = BashOperator(
    task_id ='callf1',
    bash_command = 'echo 1',
    dag = dag 
)

callf2 = BashOperator(
    task_id ='callf2',
    bash_command = 'pwd',
    dag = dag 
)



start >> callf1 >> callf2 >> end