import pandas as pd
import requests
import subprocess

# Hàm chạy lệnh CLI HDFS và lấy dữ liệu
def run_hdfs_command(command):
    try:
        # Sử dụng subprocess với shell=True để môi trường giống như dòng lệnh
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            print(f"Error: {stderr}")
            return None
        
        return stdout
    except Exception as e:
        print(f"Exception: {e}")
        return None

# Lệnh HDFS để đọc tất cả các tệp trong thư mục HDFS, nếu tên file có dạng part-*
hdfs_command = "hadoop fs -cat /odap/output_1/part-*"

# 1. Lấy danh sách file trong /odap/output_1
list_command = "hdfs dfs -ls /odap/output_1"
output = run_hdfs_command(list_command)

if output:
    print("ok")
    # 2. Lọc danh sách file
    files_to_merge = []
    for line in output.split("\n"):
        parts = line.split()
        if len(parts) >= 8 and "part" in parts[-1]:
            files_to_merge.append(parts[-1])

    if files_to_merge:
        print("Files to merge:", files_to_merge)

        # 3. Merge các file 
        files_str = " ".join(files_to_merge)
        merge_command = f"hadoop fs -cat {files_str} | hadoop fs -put - /odap/merge/merged_1.csv"
        merge_output = run_hdfs_command(merge_command)

        if merge_output is not None:
            print("Files merged successfully.")

            # 4. Xóa các file đã merge
            delete_command = f"hadoop fs -rm {files_str}"
            delete_output = run_hdfs_command(delete_command)

            if delete_output is not None:
                print("Old files deleted successfully.")
        else:
            print("Failed to merge files.")
    else:
        print("No files to merge.")
else:
    print("Failed to retrieve file list.")



