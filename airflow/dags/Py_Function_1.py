from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def func(text):
    return f'hello {text}'

text_print = 'everyone'

with DAG('print_func', 
         default_args=default_args,
         start_date=datetime(2024, 3, 1, 9),
        schedule='*/2 * * * *',
         catchup=False) as dag:

    print_func = PythonOperator(
        task_id='print',
        python_callable=func,
        op_kwargs={'text':text_print}
    )

    print_func




