from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 26),
    'retries': 1,
}

dag = DAG(
    'run_hadoop_command',
    default_args=default_args,
    description='Run Hadoop command via cmd.exe',
    schedule_interval=timedelta(days=1),  # Lập lịch hàng ngày
)

# Định nghĩa task để chạy lệnh Hadoop
run_hadoop_command = BashOperator(
    task_id='run_hadoop_command',
    bash_command='cmd.exe /C hadoop fs -mv /odap/new/part-* /odap/current/',
    dag=dag,
)

run_hadoop_command
