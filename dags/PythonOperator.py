from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime

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

def my_function1(x):
    return x + "is a must have tool for data engineers."

def my_function2(x):
    return x + "is learning Airflow"


# define DAG

dag = DAG(
    'PythonOperator',
    default_args=default_args,
    description='How to use Python Operator?',
    schedule_interval= timedelta(days=1)
)

callf1 = PythonOperator(
    task_id ='callf1',
    python_callable = my_function1,
    op_kwargs = { 'x' : "Apache Airflow"},
    dag = dag 
)

callf2 = PythonOperator(
    task_id ='callf2',
    python_callable = my_function2,
    op_kwargs = { 'x' : "Anand"},
    dag = dag 
)

callf1 >> callf2