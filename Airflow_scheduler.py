''' cái này để chạy trong máy ảo'''
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Đường dẫn tới ứng dụng Streamlit
STREAMLIT_APP_PATH = "D:/sources/Github/ODAP_CreditCards/merge.py"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'run_merge_script',
    default_args=default_args,
    description='Run Merge app using BashOperator',
    schedule_interval=timedelta(days=1),  # Lập lịch hàng ngày
    start_date=datetime(2024, 12, 25),
    catchup=False,
) as dag:
    
    # Chạy lệnh Streamlit
    run_streamlit_cmd = BashOperator(
        task_id='run_merge_task',
        # python là lệnh để chạy file Python tùy thuộc vào biến môi trường đã được cài trong máy
        bash_command=f'python {STREAMLIT_APP_PATH}',
    )

run_streamlit_cmd
