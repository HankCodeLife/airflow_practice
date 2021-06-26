from datetime import timedelta
from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
# from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
   'owner': 'hank',
    'depends_on_past': False,
    'email': ['bsexp100324@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'end_date': datetime(2020, 2, 29),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
}


dag = DAG(
    '邱邱第一個dag',
    default_args=default_args,
    description='邱邱第一個dag',
    schedule_interval='*/1 * * * *',
    start_date=days_ago(2),
    catchup=False,
    tags=['hank'],
)


def get_date(**context):
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    context['ti'].xcom_push(key='nowtime', value=date_time)


def write_my_text(**context):
    with open('/mnt/f/dags/log.txt', 'a', encoding='utf-8', newline='') as f:
        nowtime = context.get('ti').xcom_pull(key='nowtime')
        f.write(nowtime + ' dag done!\n')  
        
t1 = PythonOperator(
    task_id='get_date',
    python_callable=get_date,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id='print_text',
    python_callable=write_my_text,
    provide_context=True,
    dag=dag
)

t1 >> t2