#Importing Libraries:
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.utils.email import send_email


def failure_email(context):
    dr = context.get("dag_run")
    msg = "DAG has Failed"
    subject = f"DAG {dr} Failed"
    send_email(to='divya2103anand@gmail.com',subject=subject, html_content=msg)


# These args will get passed on to the python operator
default_args = {
    'owner': 'anand_21',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),
    'email': ['divya2103anand@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_email
    }

def success_email(context):
    dr = context.get("dag_run")
    msg = "DAG Ran Successfully"
    subject = f"DAG {dr} has completed"
    send_email(to='divya2103anand@gmail.com',subject=subject, html_content=msg)

def failure_email(context):
    dr = context.get("dag_run")
    msg = "DAG has Failed"
    subject = f"DAG {dr} Failed"
    send_email(to='divya2103anand@gmail.com',subject=subject, html_content=msg)

# define the DAG
dag = DAG(
    '8_failSuccess',
    default_args=default_args,
    description='email operator',
    schedule_interval=timedelta(days=1),
    catchup=False
)
# define the first task
start = DummyOperator(task_id='start',
        dag=dag)
sendemail = BashOperator(
    task_id ='send',
    bash_command='date',
    on_success_callback=success_email,
    dag=dag
)


sendemail1 = BashOperator(
    task_id ='send1',
    bash_command='date1',
    on_success_callback=success_email,
    dag=dag
)


end = DummyOperator(task_id='end',
        dag=dag)
start >> sendemail >> sendemail1 >> end