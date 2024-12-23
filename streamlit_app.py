import streamlit as st
import pandas as pd
from datetime import timedelta, datetime
import matplotlib.pyplot as plt
import requests
import subprocess

# Set page config
st.set_page_config(page_title="Transactions Dashboard", layout="wide")

st.write("# Transactions Dashboard")


#tao dataframe Card,Transaction Date,Transaction Time,Merchant Name,Merchant City,Amount VND
dataframe = pd.DataFrame(columns=['Card','Transaction Date','Transaction Time','Merchant Name','Merchant City','Amount VND'])
# Hàm chạy lệnh CLI HDFS và lấy dữ liệu
def run_hdfs_command(command):
    try:
        # Sử dụng subprocess với shell=True để môi trường giống như dòng lệnh
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            st.error(f"Error: {stderr}")
            return None
        
        return stdout
    except Exception as e:
        st.error(f"Exception: {e}")
        return None

# Lệnh HDFS để đọc tất cả các tệp trong thư mục HDFS, nếu tên file có dạng part-*
hdfs_command = "hadoop fs -cat /odap/output_1/part-*"

# Chạy lệnh và lấy dữ liệu
data = run_hdfs_command(hdfs_command)

# Chạy lệnh và lấy dữ liệu
raw_data = run_hdfs_command(hdfs_command)

# Kiểm tra nếu có dữ liệu trả về
if data:
    # Xử lý dữ liệu thành danh sách
    rows = data.strip().split("\n")
    data_list = []

    for line in rows:
        try:
            fields = line.split(",")
            if len(fields) == 6:  # Kiểm tra có đủ trường dữ liệu
                data_list.append({
                    'Card': fields[0],
                    'Transaction Date': fields[1],
                    'Transaction Time': fields[2],
                    'Merchant Name': fields[3],
                    'Merchant City': fields[4],
                    'Amount VND': fields[5]
                })
        except Exception as e:
            st.error(f"Error processing line: {line}. Exception: {e}")

    # Tạo DataFrame từ danh sách
    dataframe = pd.DataFrame(data_list)
else:
    st.error("No data retrieved.")

# Xóa các dòng tiêu đề lặp lại
cleaned_data = dataframe[dataframe['Transaction Date'] != 'Transaction Date']

# Reset lại chỉ mục
cleaned_data.reset_index(drop=True, inplace=True)

# Chuyen doi kieu du lieu Amount VND tu object sang float
cleaned_data['Amount VND'] = cleaned_data['Amount VND'].astype(float)

# Chuyển đổi Transaction Date và Transaction Time sang datetime
cleaned_data['Transaction DateTime'] = pd.to_datetime(cleaned_data['Transaction Date'] + ' ' + cleaned_data['Transaction Time'], format='%d/%m/%Y %H:%M:%S')

# Hiển thị DataFrame trên Streamlit
st.subheader("Cleaned Data")
st.dataframe(cleaned_data)

# Hiển thị tổng số giao dịch
st.write("## Total Transactions")
st.write(f"Total transactions: {len(cleaned_data)}")


# # Helper functions
# @st.cache_data
# def load_data():
#     data = pd.read_csv("youtube_channel_data.csv")
#     data['DATE'] = pd.to_datetime(data['DATE'])
#     data['NET_SUBSCRIBERS'] = data['SUBSCRIBERS_GAINED'] - data['SUBSCRIBERS_LOST']
#     return data

# def custom_quarter(date):
#     month = date.month
#     year = date.year
#     if month in [2, 3, 4]:
#         return pd.Period(year=year, quarter=1, freq='Q')
#     elif month in [5, 6, 7]:
#         return pd.Period(year=year, quarter=2, freq='Q')
#     elif month in [8, 9, 10]:
#         return pd.Period(year=year, quarter=3, freq='Q')
#     else:  # month in [11, 12, 1]
#         return pd.Period(year=year if month != 1 else year-1, quarter=4, freq='Q')

# def aggregate_data(df, freq):
#     if freq == 'Q':
#         df = df.copy()
#         df['CUSTOM_Q'] = df['DATE'].apply(custom_quarter)
#         df_agg = df.groupby('CUSTOM_Q').agg({
#             'VIEWS': 'sum',
#             'WATCH_HOURS': 'sum',
#             'NET_SUBSCRIBERS': 'sum',
#             'LIKES': 'sum',
#             'COMMENTS': 'sum',
#             'SHARES': 'sum',
#         })
#         return df_agg
#     else:
#         return df.resample(freq, on='DATE').agg({
#             'VIEWS': 'sum',
#             'WATCH_HOURS': 'sum',
#             'NET_SUBSCRIBERS': 'sum',
#             'LIKES': 'sum',
#             'COMMENTS': 'sum',
#             'SHARES': 'sum',
#         })

# def get_weekly_data(df):
#     return aggregate_data(df, 'W-MON')

# def get_monthly_data(df):
#     return aggregate_data(df, 'M')

# def get_quarterly_data(df):
#     return aggregate_data(df, 'Q')

# def format_with_commas(number):
#     return f"{number:,}"

# def create_metric_chart(df, column, color, chart_type, height=150, time_frame='Daily'):
#     chart_data = df[[column]].copy()
#     if time_frame == 'Quarterly':
#         chart_data.index = chart_data.index.strftime('%Y Q%q ')
#     if chart_type=='Bar':
#         st.bar_chart(chart_data, y=column, color=color, height=height)
#     if chart_type=='Area':
#         st.area_chart(chart_data, y=column, color=color, height=height)

# def is_period_complete(date, freq):
#     today = datetime.now()
#     if freq == 'D':
#         return date.date() < today.date()
#     elif freq == 'W':
#         return date + timedelta(days=6) < today
#     elif freq == 'M':
#         next_month = date.replace(day=28) + timedelta(days=4)
#         return next_month.replace(day=1) <= today
#     elif freq == 'Q':
#         current_quarter = custom_quarter(today)
#         return date < current_quarter

# def calculate_delta(df, column):
#     if len(df) < 2:
#         return 0, 0
#     current_value = df[column].iloc[-1]
#     previous_value = df[column].iloc[-2]
#     delta = current_value - previous_value
#     delta_percent = (delta / previous_value) * 100 if previous_value != 0 else 0
#     return delta, delta_percent

# def display_metric(col, title, value, df, column, color, time_frame):
#     with col:
#         with st.container(border=True):
#             delta, delta_percent = calculate_delta(df, column)
#             delta_str = f"{delta:+,.0f} ({delta_percent:+.2f}%)"
#             st.metric(title, format_with_commas(value), delta=delta_str)
#             create_metric_chart(df, column, color, time_frame=time_frame, chart_type=chart_selection)
            
#             last_period = df.index[-1]
#             freq = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'M', 'Quarterly': 'Q'}[time_frame]
#             if not is_period_complete(last_period, freq):
#                 st.caption(f"Note: The last {time_frame.lower()[:-2] if time_frame != 'Daily' else 'day'} is incomplete.")

# # Load data
# df = load_data()

# Set up input widgets
# st.logo(image="images/streamlit-logo-primary-colormark-lighttext.png", 
#         icon_image="images/streamlit-mark-color.png")

with st.sidebar:
    st.title("YouTube Channel Dashboard")
    st.header("⚙️ Settings")
    
    # max_date = df['DATE'].max().date()
    # default_start_date = max_date - timedelta(days=365)  # Show a year by default
    # default_end_date = max_date
    # start_date = st.date_input("Start date", default_start_date, min_value=df['DATE'].min().date(), max_value=max_date)
    # end_date = st.date_input("End date", default_end_date, min_value=df['DATE'].min().date(), max_value=max_date)
    # time_frame = st.selectbox("Select time frame",
    #                           ("Daily", "Weekly", "Monthly", "Quarterly"),
    # )
    # chart_selection = st.selectbox("Select a chart type",
    #                                ("Bar", "Area"))

# # Prepare data based on selected time frame
# if time_frame == 'Daily':
#     df_display = df.set_index('DATE')
# elif time_frame == 'Weekly':
#     df_display = get_weekly_data(df)
# elif time_frame == 'Monthly':
#     df_display = get_monthly_data(df)
# elif time_frame == 'Quarterly':
#     df_display = get_quarterly_data(df)

# # Display Key Metrics
# st.subheader("All-Time Statistics")

# metrics = [
#     ("Total Subscribers", "NET_SUBSCRIBERS", '#29b5e8'),
#     ("Total Views", "VIEWS", '#FF9F36'),
#     ("Total Watch Hours", "WATCH_HOURS", '#D45B90'),
#     ("Total Likes", "LIKES", '#7D44CF')
# ]

# cols = st.columns(4)
# for col, (title, column, color) in zip(cols, metrics):
#     total_value = df[column].sum()
#     display_metric(col, title, total_value, df_display, column, color, time_frame)

# st.subheader("Selected Duration")

# if time_frame == 'Quarterly':
#     start_quarter = custom_quarter(start_date)
#     end_quarter = custom_quarter(end_date)
#     mask = (df_display.index >= start_quarter) & (df_display.index <= end_quarter)
# else:
#     mask = (df_display.index >= pd.Timestamp(start_date)) & (df_display.index <= pd.Timestamp(end_date))
# df_filtered = df_display.loc[mask]

# cols = st.columns(4)
# for col, (title, column, color) in zip(cols, metrics):
#     display_metric(col, title.split()[-1], df_filtered[column].sum(), df_filtered, column, color, time_frame)

# # DataFrame display
# with st.expander('See DataFrame (Selected time frame)'):
#     st.dataframe(df_filtered)

# Chart display
