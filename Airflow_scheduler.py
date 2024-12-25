''' cái này để chạy trong máy ảo'''
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Đường dẫn tới ứng dụng Streamlit
STREAMLIT_APP_PATH = "D:/sources/Github/ODAP_CreditCards/streamlit_app.py"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'run_streamlit',
    default_args=default_args,
    description='Run Streamlit app using BashOperator',
    schedule_interval=timedelta(days=1),  # Lập lịch hàng ngày
    start_date=datetime(2024, 12, 25),
    catchup=False,
) as dag:
    
    # Chạy lệnh Streamlit
    run_streamlit_cmd = BashOperator(
        task_id='run_streamlit_task',
        bash_command=f'streamlit run {STREAMLIT_APP_PATH}',
    )

    run_streamlit_cmd
