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
hdfs_command = "hadoop fs -cat /aida/output/part-*"

# 1. Lấy danh sách file trong /odap/output_1
list_command = "hdfs dfs -ls /aida/output"
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

        # timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        # timestamp_String = str(timestamp)
        # chia nhỏ thoe nhóm 10 file
        step = 100
        for i in range(0, len(files_to_merge), step):
            if i+step > len(files_to_merge):
                files = files_to_merge[i:]
            else:
                files = files_to_merge[i:i+step]

            files_str = " ".join(files)
            # move file to merge
            move_command = f"hadoop fs -mv {files_str} /aida/merge"
            move_output = run_hdfs_command(move_command)
            if move_output is not None:
                print(f"Files moved successfully.")
            else:
                print(f"Failed to move files {i}-{i+step}.")
            
            # merge_command = f"hadoop fs -cat {files_str} | hadoop fs -put - /aida/merge/merged-{timestamp_String}-{i}.csv"
            # merge_output = run_hdfs_command(merge_command)

            # if merge_output is not None:
            #     print(f"Files {i}-{i+step} merged successfully.")
            # else:
            #     print(f"Failed to merge files {i}-{i+step}.")
        # # 3. Merge các file 
        # files_str = " ".join(files_to_merge)
        # merge_command = f"hadoop fs -cat {files_str} | hadoop fs -put - /aida/merge/merged-{timestamp_String}.csv"
        # merge_output = run_hdfs_command(merge_command)
            
    else:
        print("No files to merge.")
else:
    print("Failed to retrieve file list.")



