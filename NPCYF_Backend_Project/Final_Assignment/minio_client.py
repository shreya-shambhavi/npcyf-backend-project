from minio import Minio
from config import settings
from fastapi import UploadFile, HTTPException
import io
import pandas as pd

minio_client = Minio(
    endpoint = settings.MINIO_ENDPOINT,
    access_key = settings.MINIO_ACCESS_KEY,
    secret_key = settings.MINIO_SECRET_KEY,
    secure = False
)

try:
    if not minio_client.bucket_exists(settings.MINIO_BUCKET):
        minio_client.make_bucket(settings.MINIO_BUCKET)
    print(f"Successfully connected to bucket '{settings.MINIO_BUCKET}'")

except Exception as e:
    print(f"Could not connect to MinIO: {e}")
    raise HTTPException(status_code = 500, detail = "MinIO connection failed")

# Upload file to MinIO
def upload_to_minio(file: UploadFile, file_name: str) -> None:
    
    try:
        file_content = file.file.read()
        file_size = len(file_content)
        
        file_stream = io.BytesIO(file_content)
        file_stream.seek(0)
        
        minio_client.put_object(
            settings.MINIO_BUCKET,
            file_name,
            file_stream,
            length = file_size
        )
        print(f"Successfully uploaded {file_name} to MinIO.")
        
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"MinIO upload failed: {e}")

# Download file from MinIO
def download_from_minio(file_name: str) -> pd.DataFrame:
    
    try:
        response = minio_client.get_object(settings.MINIO_BUCKET, file_name)
        
        file_data = response.read()
        file_extension = file_name.split('.')[-1]
        
        if file_extension == "csv":
            df = pd.read_csv(io.BytesIO(file_data))
        elif file_extension == "xlsx":
            df = pd.read_excel(io.BytesIO(file_data))
        else:
            print(f"Unsupported file format for download: {file_extension}")
            raise HTTPException(status_code = 400, detail = "Unsupported file format for download.")
        
        return df
    
    except Exception as e:
        print(f"Error occurred while downloading file from MinIO: {e}")
        raise HTTPException(status_code = 500, detail = f"MinIO download failed for {file_name}: {e}")
    finally:
        if 'response' in locals() and response:
            response.close()
            response.release_conn()

# Save merged DataFrame back to MinIO
def upload_merged_to_minio(df: pd.DataFrame, merged_file_name: str, file_format: str) -> None:
    
    try:
        file_stream = io.BytesIO()
        
        if file_format == "csv":
            df.to_csv(file_stream, index = False)
        elif file_format == "xlsx":
            df.to_excel(file_stream, index = False)
        else:
            print(f"Unsupported file format for upload: {file_format}")
            raise HTTPException(status_code = 400, detail = "Unsupported file format for upload.")
        
        file_stream.seek(0)
        file_size = file_stream.getbuffer().nbytes
        
        minio_client.put_object(
            settings.MINIO_BUCKET,
            merged_file_name,
            file_stream,
            length = file_size
        )
        print(f"Successfully uploaded merged file {merged_file_name} to MinIO.")
        
    except Exception as e:
        print(f"Error occurred while uploading merged file to MinIO: {e}")
        raise HTTPException(status_code = 500, detail = f"MinIO upload failed for merged file: {e}")

