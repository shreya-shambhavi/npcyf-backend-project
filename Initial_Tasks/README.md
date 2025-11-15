# **NPCYF - Backend Project**

## Initial Assigned Tasks

To run the two tasks, kindly follow the following steps:

1. **Create and activate a virtual environment in the project root folder**:

   ```bash
   python -m venv venv         # Create virtual environment (On Windows)
   python3 -m venv venv        # Create virtual environment (On Linux/Mac)

   venv\Scripts\activate       # Activate virtual environment (On Windows)
   source venv/bin/activate    # Activate virtual environment (On Linux/Mac)

2. **Install required dependencies from requirements.txt**:

   ```bash
   pip install -r requirements.txt

3. **To run Task 1**

   ```bash
   cd task1
   python main.py file1 file2 jointype

4. **To run Task 2**

   ```bash
   cd task2
   uvicorn main:app --reload

5. **To run Task 3: First install MinIO on the system, follow up via installing MinIO in the vitual environment and finally run the command on CLI**

   ```bash
   mkdir C:\minio        # Install MinIO on system. All the steps mentioned here are Windows specific. Please checkout the website for other OS specific instructions.    
   Invoke-WebRequest -Uri "https://dl.min.io/server/minio/release/windows-amd64/minio.exe" -OutFile "C:\minio\minio.exe"
   mkdir C:\minio\data
   .\minio.exe server C:\minio\data --console-address ":9001"

   ```bash
   pip install minio            # Install MinIO in the virtual environment, iff haven't done it already. Do check via pip list command. 

   ```bash
   cd task3                # running via CLI
   python main.py file1 file2 bucketname jointype

   cd task3               # running via uvicorn server
   uvicorn main:app --reload
