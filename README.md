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


## Technical Requirements

### Prerequisites
- JDK (Java Development Kit)
- Apache Kafka (https://kafka.apache.org/downloads)(https://kafka.apache.org/quickstart)
- Hadoop
- Apache Spark, PySpark
- AirFlow
- Streamlit

### Setup Instructions


## Chức năng và 
1. 

2. Key Reports
   - State-wise AQI trends
   - Quarterly analysis
   - Air quality category distribution
   - Regional comparisons
   - Special focus on Hawaii, Alaska, Illinois, and Delaware

3. Data Mining
   - Predictive models for future air quality
   - Pattern analysis and trending
   - Seasonal variations study

## Project Timeline
- Project Start: 
- Midterm Q&A: 
- Final Submission: 
- Final Q&A: 

## Documentation
- Technical documentation is available in `/docs/`
- Presentation video (10 minutes) available in `/docs/presentations/`
- Full analysis report in `/docs/reports/`

## Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## References
1. [Air Quality Index - Wikipedia](https://en.wikipedia.org/wiki/Air_quality_index)
2. [US Counties Database](https://simplemaps.com/data/us-counties)
3. [EPA AirData Documentation](https://www.epa.gov/outdoor-air-quality-data)

## License
This project is created for educational purposes as part of the CSC12107 – Information System for Business Intelligence course at University of Science - VNUHCM.

## Acknowledgments
- Course Instructors:
  - Hồ Thị Hoàng Vy (hthvy@fit.hcmus.edu.vn)
  - Tiết Gia Hồng (tghong@fit.hcmus.edu.vn)
  - Nguyễn Ngọc Minh Châu (nnmchau@fit.hcmus.edu.vn) 
