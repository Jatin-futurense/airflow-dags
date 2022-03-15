from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta,datetime

default_args = {
    'owner':'anand_21',
    'depends_on_past': False,
    'start_date':datetime(2022,3,1),
    'email':['divya2103anand@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries': 1,
    'retry_delay' :timedelta(minutes=1)
}

dag = DAG(
    'Spark_Submit_Operator',
    default_args=default_args,
    description='How to use Spark Operator?',
    schedule_interval=timedelta(days=1)
)

spark_submit_operator = SparkSubmitOperator(
    conn_id='spark-default',
    application='/home/saif/PycharmProjects/c9/assign14.py',
    task_id = 'spark_submit_operator',
    dag = dag
)