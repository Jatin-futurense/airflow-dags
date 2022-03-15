from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import timedelta,datetime
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner':'anand_21',
    'depends_on_past':False,
    'start_date':datetime(2022,3,10),
    'email':['divya2103anand@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    '12EmailOperator',
    default_args=default_args,
    description='How to use Email Operator?',
    schedule_interval= timedelta(days=1),
    catchup=False
)

start = DummyOperator(
    task_id = 'start',
    dag = dag
)

end = DummyOperator(
    task_id = 'end',
    dag = dag
)

sendEmail = EmailOperator(
    task_id = 'sendEmail',
    to = ['lucy04081997@gmail.com'],
    subject = 'Airflow Mail Testing',
    html_content = """ <h3> Hi Lucy ,How r u ? </h3> """,
    dag = dag
)

start >> sendEmail >> end
