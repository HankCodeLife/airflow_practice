from datetime import timedelta
from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago


default_args = {
   'owner': 'hank',
    'depends_on_past': False,
    'email': ['bsexp100324@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    '邱邱第2個dag',
    default_args=default_args,
    description='邱邱第2個dag',
    schedule_interval='*/1 * * * *',
    start_date=days_ago(2),
    catchup=False,
    tags=['hank'],
)


def get_date(**context):
    now = datetime.now()
    now_time_str = now.strftime("%m/%d/%Y, %H:%M:%S")
    now_minute = now.minute
    context['ti'].xcom_push(key='now_time_str', value=now_time_str)
    context['ti'].xcom_push(key='now_minute', value=now_minute)

def branch_minute(**context):
    now_minute = context.get('ti').xcom_pull(key='now_minute')
    
    if now_minute%2 == 0:
        return 'print_date1'
    else:
        return 'print_date2'

def print_date1(**context):
     with open('/mnt/f/dags/log.txt', 'a', encoding='utf-8', newline='') as f:
        nowtime = context.get('ti').xcom_pull(key='now_time_str')
        f.write('偶數 - ' + nowtime + ' dag done!\n')  
   
def print_date2(**context):
     with open('/mnt/f/dags/log.txt', 'a', encoding='utf-8', newline='') as f:
        nowtime = context.get('ti').xcom_pull(key='now_time_str')
        f.write('單數 - ' + nowtime + ' dag done!\n')

t1 = PythonOperator(
    task_id='get_date',
    python_callable=get_date,
    provide_context=True,
    dag=dag
)

t2 = BranchPythonOperator(
    task_id='branch_minute',
    python_callable=branch_minute,
    provide_context=True,
    dag=dag
)

t31 = PythonOperator(
    task_id='print_date1',
    python_callable=print_date1,
    provide_context=True,
    dag=dag
)

t32 = PythonOperator(
    task_id='print_date2',
    python_callable=print_date2,
    provide_context=True,
    dag=dag
)

t1 >> t2 >> [t31, t32]