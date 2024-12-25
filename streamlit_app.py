import streamlit as st
import pandas as pd
from datetime import timedelta, datetime
import matplotlib.pyplot as plt
import requests
import subprocess
import altair as alt

# =================== Set page config =======================
st.set_page_config(page_title="Transactions Dashboard", layout="wide")


# =================== Helper functions =======================
@st.cache_data
def load_data():
    ''' 
    Load data from HDFS and return a cleaned DataFrame 
    Return a DataFrame with the following columns
    '''

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
    hdfs_command = "hadoop fs -cat /aida/output_1/part-*"

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
    cleaned_data['Transaction Date'] = pd.to_datetime(cleaned_data  ['Transaction Date'], format='%d/%m/%Y')

    return cleaned_data

def format_number(value, max_length=10):
    # Chuyển số thành chuỗi có dấu phẩy phân cách
    formatted = f"{value:,.0f}"
    # Nếu chuỗi dài hơn max_length, rút gọn và thêm "..."
    if len(formatted) > max_length:
        return formatted[:max_length - 3] + "..."
    return formatted


def total_stats(df, time_frame='Daily'):
    # Hàm nhóm dữ liệu theo thời gian
    def group_data_by_time(column, agg_func="count"):
        if time_frame == "Daily":
            grouped = df.groupby(df["Transaction Date"].dt.date)[column].agg(agg_func).reset_index()
        elif time_frame == "Monthly":
            grouped = df.groupby(df["Transaction Date"].dt.to_period("M"))[column].agg(agg_func).reset_index()
            grouped["Transaction Date"] = grouped["Transaction Date"].dt.to_timestamp()
        elif time_frame == "Yearly":
            grouped = df.groupby(df["Transaction Date"].dt.to_period("Y"))[column].agg(agg_func).reset_index()
            grouped["Transaction Date"] = grouped["Transaction Date"].dt.to_timestamp()
        return grouped
    
    total1, total2, total3, total4 = st.columns(4)

    with total1:
        with st.container(border=True):
            st.metric(label="Total Transactions", value=len(df), delta_color="normal")

            # Biểu đồ cho Total Transactions
            transactions_data = group_data_by_time("Amount VND", agg_func="count")
           
            # Tạo biểu đồ Altair và ẩn nhãn trục
            chart = alt.Chart(transactions_data).mark_area(opacity=0.5, color='#7BD3EA').encode(
                x=alt.X("Transaction Date:T", axis=alt.Axis(labels=False, title=None)),  # Ẩn nhãn trục X
                y=alt.Y("Amount VND:Q", axis=alt.Axis(labels=False, title=None))  # Ẩn nhãn trục Y
            ).properties(
                width=800, height=100
            )

            # Hiển thị biểu đồ Altair
            st.altair_chart(chart, use_container_width=True)
            
    with total2:
        with st.container(border=True):
            formatted_value = format_number(df["Amount VND"].sum())
            st.metric(label="Total Amount", value=f'{formatted_value} VND', delta_color="normal")

            # Biểu đồ cho Total Amount
            amount_data = group_data_by_time("Amount VND", agg_func="sum")
            chart = alt.Chart(amount_data).mark_area(opacity=0.5, color='#F6D6D6').encode(
                x=alt.X("Transaction Date:T", axis=alt.Axis(labels=False, title=None)),
                y=alt.Y("Amount VND:Q", axis=alt.Axis(labels=False, title=None))
            ).properties(
                width=800, height=100
            )
            st.altair_chart(chart, use_container_width=True)

    with total3:
        with st.container(border=True):
            st.metric(label="Total Merchants", value=df["Merchant Name"].nunique(), delta_color="normal")

            # Biểu đồ cho Total Merchants
            merchant_data = group_data_by_time("Merchant Name", agg_func="nunique")
            chart = alt.Chart(merchant_data).mark_area(opacity=0.5, color='#F6F7C4').encode(
                x=alt.X("Transaction Date:T", axis=alt.Axis(labels=False, title=None)),
                y=alt.Y("Merchant Name:Q", axis=alt.Axis(labels=False, title=None))
            ).properties(
                width=800, height=100
            )
            st.altair_chart(chart, use_container_width=True)

    with total4:
        with st.container(border=True):
            st.metric(label="Total Cities", value=df["Merchant City"].nunique(), delta_color="normal")
            
            # Biểu đồ cho Total Cities
            city_data = group_data_by_time("Merchant City", agg_func="nunique")
            chart = alt.Chart(city_data).mark_area(opacity=0.5, color='#A1EEBD').encode(
                x=alt.X("Transaction Date:T", axis=alt.Axis(labels=False, title=None)),
                y=alt.Y("Merchant City:Q", axis=alt.Axis(labels=False, title=None))
            ).properties(
                width=800, height=100
            )
            st.altair_chart(chart, use_container_width=True)
    

def plot_transaction_count_by_merchant(stats):
    st.subheader("Total Transactions by Merchant")
    line = alt.Chart(stats).mark_line().encode(
        alt.X('Transaction Date', title='Time'),
        alt.Y('Total Transaction', title='Total Transaction'),
        color='Merchant Name'
    ).properties(
        width=400,
        height=300
    ).configure_title(
        fontSize=15,
        anchor='middle'
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).interactive()
    st.altair_chart(line, use_container_width=True)
    

def plot_total_amount_by_merchant(stats):
    st.subheader("Total Amount by Merchant")
    line = alt.Chart(stats).mark_line().encode(
        alt.X('Transaction Date', title='Time'),
        alt.Y('Total Amount VND', title='Total Amount'),
        color='Merchant Name'
    ).properties(
        width=400,
        height=300
    ).configure_title(
        fontSize=15,
        anchor='middle'
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).interactive()
    st.altair_chart(line, use_container_width=True)

# Hàm vẽ Line Chart theo thời gian 
def vis(col, df, time_frame, number):
    with col:
        with st.container(border=True):
            # Thống kê theo ngày
            if time_frame == 'Daily':
                transaction_df = df.groupby([df['Transaction Date'].dt.date, 'Merchant Name']).size().reset_index(name='Total Transaction')  

                amount_df = df.groupby([df['Transaction Date'].dt.date, 'Merchant Name'])['Amount VND'].sum().reset_index(name='Total Amount VND')
            # Thống kê theo tháng
            elif time_frame == 'Monthly':
                transaction_df = df.groupby([df['Transaction Date'].dt.to_period('M'), 'Merchant Name']).size().reset_index(name='Total Transaction')
                transaction_df['Transaction Date'] = transaction_df['Transaction Date'].dt.to_timestamp()

                amount_df = df.groupby([df['Transaction Date'].dt.to_period('M'), 'Merchant Name'])['Amount VND'].sum().reset_index(name='Total Amount VND')
                amount_df['Transaction Date'] = amount_df['Transaction Date'].dt.to_timestamp()
            # Thống kê theo năm
            elif time_frame == 'Yearly':
                transaction_df = df.groupby([df['Transaction Date'].dt.year, 'Merchant Name']).size().reset_index(name='Total Transaction')
                transaction_df['Transaction Date'] = pd.to_datetime(transaction_df['Transaction Date'], format='%Y')

                amount_df = df.groupby([df['Transaction Date'].dt.year, 'Merchant Name'])['Amount VND'].sum().reset_index(name='Total Amount VND')
                amount_df['Transaction Date'] = pd.to_datetime(amount_df['Transaction Date'], format='%Y')
            else:
                raise ValueError("Invalid time_frame")
            
            # Vẽ biểu đồ
            if number == 1:
                plot_transaction_count_by_merchant(transaction_df)
            elif number == 2:
                plot_total_amount_by_merchant(amount_df)
            

# Load dataframe
df = load_data()

#======================= Sidebar =======================
with st.sidebar:
    st.title("Transactions Dashboard")
    st.header("⚙️ Settings")
    
    time_frame = st.selectbox("Select time frame",
                              ("Daily", "Monthly", "Yearly"), index=2)

    all_merchants = df["Merchant Name"].unique()
    select_all = st.checkbox("Select All Merchants", value=True)

    if select_all:
        selected_merchants = all_merchants
    else:
        selected_merchants = st.multiselect(
            "Select Merchant(s):",
            options=all_merchants,
            default=[]
        )    

# Lọc dữ liệu theo Merchant Name được chọn
filtered_df = df[df["Merchant Name"].isin(selected_merchants)] 

# Display Key Metrics
st.subheader("Key Metrics")
total_stats(filtered_df, time_frame)

# Display Key Metrics
st.subheader("All-Time Statistics")

col1, col2 = st.columns(2)
vis(col1, filtered_df, time_frame, 1)
vis(col2, filtered_df, time_frame, 2)

#================== DataFrame display =======================
with st.expander('See DataFrame'):
    st.dataframe(filtered_df)

