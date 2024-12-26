
import csv
import time
import random
import signal
import sys
from confluent_kafka import Producer


# .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
# .\bin\windows\kafka-server-start.bat .\config\server.properties

# Cấu hình producer
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka broker
    'client.id': 'transactions-producer',  # ID của producer
}

# Khởi tạo producer
producer = Producer(producer_config)
topic = 'transactions'

# Hàm xử lý khi nhận tín hiệu Ctrl + C
def shutdown_producer(signal, frame):
    print("\nFlushing and closing producer...")
    producer.flush()  # Gửi các thông điệp còn lại
    print("Producer closed. Exiting program.")
    sys.exit(0)

# Đăng ký xử lý tín hiệu ngắt (Ctrl + C)
signal.signal(signal.SIGINT, shutdown_producer)

# Đọc file CSV và gửi dữ liệu
try:
    with open('data\\credit_card_transactions-ibm_v2.csv', mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.produce(topic, value=str(row).encode('utf-8'))
            print(f"Sent: {row}")
            time.sleep(random.uniform(1, 3))  # Delay ngẫu nhiên từ 1-3s
except Exception as e:
    print(f"Error: {e}")
finally:
    shutdown_producer(None, None)
