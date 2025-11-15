from minio import Minio
from fastapi import FastAPI
import uvicorn
import pandas as pd
import sys

app = FastAPI()

MINIO_ENDPOINT = "localhost:9000"
MINIO_USERNAME = "minioadmin"
MINIO_PASSWORD = "minioadmin"

minio = Minio(
    endpoint = MINIO_ENDPOINT,
    access_key = MINIO_USERNAME,
    secret_key = MINIO_PASSWORD,
    secure = False
)

def minio_merge(file1, file2, bucket_name, join_type):

    if not minio.bucket_exists(bucket_name):
        print(f"Minio Bucket '{bucket_name}' does not exist.")
        return
    
    if not minio.stat_object(bucket_name, file1):
        print(f"File '{file1}' does not exist in bucket '{bucket_name}'.")
        return
    
    if not minio.stat_object(bucket_name, file2):
        print(f"File '{file2}' does not exist in bucket '{bucket_name}'.")
        return 
    
    if join_type not in ['inner', 'outer', 'left', 'right']:
        print(f"Invalid join type '{join_type}'. Kindly choose from 'inner', 'outer', 'left', 'right'.")
        return

    minio.fget_object(bucket_name, file1, file1)
    minio.fget_object(bucket_name, file2, file2)

    def read_file(path):
        if path.endswith('.csv'):
            return pd.read_csv(path)
        elif path.endswith(('.xlsx')):
            return pd.read_excel(path)
        else:
            raise ValueError("Unsupported file format. Kindly use CSV or Excel files.")

    datafile1 = read_file(file1)
    datafile2 = read_file(file2) 

    datafile1.columns = datafile1.columns.str.strip().str.lower()
    datafile2.columns = datafile2.columns.str.strip().str.lower()  

    if 'id' not in datafile1.columns or 'id' not in datafile2.columns:
        print("Error: Column 'id' not found in one or both files.")
        print(f"File1 columns: {list(datafile1.columns)}")
        print(f"File2 columns: {list(datafile2.columns)}")
        return

    new_merged_file = pd.merge(datafile1, datafile2, on = 'id', how = join_type)

    output_filename = f"new_merged_file_by_{join_type}_join"

    if file1.endswith('.csv'):
        merged_file = f"{output_filename}.csv"
        new_merged_file.to_csv(merged_file, index = False)
    else:
        merged_file = f"{output_filename}.xlsx"
        new_merged_file.to_excel(merged_file, index = False)

    minio.fput_object(bucket_name, merged_file, merged_file)

    print(f"Merged file '{merged_file}' uploaded to bucket '{bucket_name}' by merging {file1} and {file2}.")
    return merged_file

@app.get("/")
async def root():
    return {"message": "Hieee, Welcome to the Task 3 - MinIO File Merger API. Use the /minio_merge endpoint to merge two csv/excel files stored in a MinIO bucket on the basis of 'id'. Also, kindly use Swagger docs at /docs for all test purposes."}

@app.post("/minio_merge")
async def minio_merge_api(file1: str, file2: str, bucket_name: str, join_type: str):
    merged_file = minio_merge(file1, file2, bucket_name, join_type)
    return {"message": f"File merge and upload completed. The name of the merged file is {merged_file}. Check Minio bucket for the merged file."}


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] != "main:app":

        if len(sys.argv) != 5:
            print("Sample Usage: python main.py <file1> <file2> <bucket_name> <join_type>")
            sys.exit(1)

        file1 = sys.argv[1]
        file2 = sys.argv[2]
        bucket_name = sys.argv[3]
        join_type = sys.argv[4].lower()

        minio_merge(file1, file2, bucket_name, join_type)
    else:
        uvicorn.run(app, host="0.0.0.0", port=8000)
