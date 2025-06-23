# Credit Card Transaction Data Management System

## Problem Description

A financial company wants to build a real-time credit card transaction data management system. Transaction data is generated from POS machines at retail stores, restaurants, and other cashless payment locations. The system needs to process data in real-time. When a transaction occurs, the data is sent to the system to check for errors. If Is Fraud = Yes, the transaction is flagged as failed and no further processing is required.

For successful transactions, the system will store the following information:
- **Credit Card**: Credit card details
- **Ngày giao dịch**: Format `dd/mm/yyyy`.  
- **Thời gian**: Format `hh:mm:ss`.  
- **Merchant name**: Name of the business where the transaction occurred
- **Merchant City**: City where the transaction took place
- **Số tiền**: Converted to VND using daily exchange rates

At the end of each day, all transactions will be summarized as follows:
- Total transaction value by `merchant name`.  
- Transaction count by `merchant name`.  
- Daily, monthly, and yearly statistics

All statistical information will be visualized using specialized tools or systems.

## Thông tin nhóm
- Trần Thị Kim Trinh - 21120580
- Đinh Hoàng Trung - 21120582
- Nguyễn Thủy Uyên - 21120590


## Data Source
Provided by the instructor as a CSV file containing transaction records

### Prerequisites
- JDK (Java Development Kit)
- Apache Kafka (https://kafka.apache.org/downloads)(https://kafka.apache.org/quickstart)
- Hadoop
- Apache Spark, PySpark
- AirFlow
- Streamlit

### Setup Instructions

## System Architecture
1. Data Pipeline: Apache Kafka
   - Producer: Reads data from CSV file and sends to "transaction" topic (1-3s delay)
   - Consumer: Receives data and processes with Spark Streaming
2. Data Processing: Apache Spark Streaming
   - Parse JSON data into DataFrame
   - Filter online transactions and non-Fraud transactions
   - Convert date/time formats and currency (to VND)
   - Select key fields for storage
3. Data Storage: Hadoop
   - New directory: Stores newly updated daily data
   - Current directory: Stores processed data for visualization
4. Visualization: Streamlit
5. Workflow Scheduling: Apache Airflow

## Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## References
1. Official documentation for Apache Kafka, Spark, Hadoop
2. Streamlit and Airflow guides

## License
This project was created for educational purposes as part of the "Real-time Data Processing and Analysis" course at University of Science - VNUHCM.

## Acknowledgments
- Course Instructors:
  - Nguyễn Trần Minh Thư (ntmthu@fit.hcmus.edu.vn)
  - Phạm Minh Tú (pmtu@fit.hcmus.edu.vn)
