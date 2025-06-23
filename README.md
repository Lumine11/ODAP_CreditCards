# Hệ Thống QUản Lý Dữ Liệu Giao Dịch Thông Qua Thẻ Tín Dụng (Credit Card).

## Mô tả bài toán  

Một công ty tài chính muốn xây dựng hệ thống quản lý dữ liệu giao dịch thông qua thẻ tín dụng (credit card). Dữ liệu được phát sinh từ các máy POS đặt tại các cửa hàng mua sắm, nhà hàng, hoặc bất kỳ nơi nào thanh toán không dùng tiền mặt. Công ty muốn xây dựng hệ thống xử lý dữ liệu theo thời gian thực. Khi một giao dịch được phát sinh, dữ liệu được gửi đến hệ thống, tiến hành kiểm tra dữ liệu có lỗi hay không? Nếu `Is Fraud = Yes`, xác định lỗi và giao dịch này xem như không thành công, không cần xử lý tiếp.  

Khi một giao dịch thành công, hệ thống sẽ lưu trữ các thông tin sau:  
- **Credit Card**: Thông tin thẻ tín dụng.  
- **Ngày giao dịch**: Định dạng `dd/mm/yyyy`.  
- **Thời gian**: Định dạng `hh:mm:ss`.  
- **Merchant name**: Tên nơi xảy ra giao dịch.  
- **Merchant City**: Thành phố nơi giao dịch.  
- **Số tiền**: Chuyển sang VNĐ theo tỉ giá được cập nhật mỗi ngày.  

Cuối ngày, tất cả giao dịch sẽ được thống kê như sau:  
- Tổng giá trị giao dịch theo từng `merchant name`.  
- Đếm số lượng giao dịch của mỗi `merchant name`.  
- Thống kê theo ngày, tháng và năm.  

Tất cả thông tin thống kê này sẽ được trực quan hóa qua công cụ hoặc hệ thống chuyên biệt.
## Thông tin nhóm
- Trần Thị Kim Trinh - 21120580
- Đinh Hoàng Trung - 21120582
- Nguyễn Thủy Uyên - 21120590


## Data Source
Được cung cấp bởi giảng viên thực hành gồm File CSV chứa thông tin giao dịch

### Prerequisites
- JDK (Java Development Kit)
- Apache Kafka (https://kafka.apache.org/downloads)(https://kafka.apache.org/quickstart)
- Hadoop
- Apache Spark, PySpark
- AirFlow
- Streamlit

### Setup Instructions

## Kiến trúc hệ thống
1. Công nghệ sử dụng
   - Truyền tải dữ liệu: Apache Kafka
      - Producer: Đọc dữ liệu từ file CSV và gửi đến topic "transaction" (độ trễ 1-3s)
      - Consumer: Nhận dữ liệu và xử lý bằng Spark Streaming
   - Xử lý dữ liệu: Apache Spark Streaming
      - Parse dữ liệu JSON thành DataFrame
      - Lọc giao dịch online và không phải Fraud
      - Chuyển đổi định dạng ngày/thời gian và tiền tệ (sang VND)
      - Chọn các trường quan trọng để lưu trữ
   - Lưu trữ dữ liệu: Hadoop
      - Thư mục new: Chứa dữ liệu mới (cập nhật hàng ngày)
      - Thư mục current: Chứa dữ liệu đã xử lý để trực quan hóa
   - Trực quan hóa: Streamlit
   - Lập lịch công việc: Apache Airflow

## Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## References
1. Tài liệu chính thức của Apache Kafka, Spark, Hadoop
2. Tài liệu hướng dẫn Streamlit và Airflow

## Giấy phép
Dự án được thực hiện cho mục đích học tập trong khuôn khổ môn học "Xử Lí Phân Tích Dữ Liệu Trực Tuyến" tại Trường Đại Học Khoa Học Tự Nhiên - ĐHQG HCM.

## Acknowledgments
- Course Instructors:
  - Hồ Thị Hoàng Vy (hthvy@fit.hcmus.edu.vn)
  - Tiết Gia Hồng (tghong@fit.hcmus.edu.vn)
  - Phạm Minh Tú (pmtu@fit.hcmus.edu.vn)
