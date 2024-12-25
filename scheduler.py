import os
import subprocess
import papermill as pm

def run_notebook():
    # Copy notebook từ máy chính qua máy ảo
    subprocess.run(["scp", "user@host:/path/to/notebook.ipynb", "/tmp/notebook.ipynb"])
    
    # Chạy notebook
    pm.execute_notebook(
        "/tmp/notebook.ipynb",
        "/tmp/notebook_output.ipynb"
    )
