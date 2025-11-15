from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, Query
from sqlalchemy.orm import Session
import crud
import schemas
from database import get_db
from minio_client import upload_to_minio, download_from_minio, upload_merged_to_minio
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
import pandas as pd
import uuid
import json
import io

ALLOWED_EXTENSIONS = {"csv", "xlsx"}

router = APIRouter()

# POST Method — File Upload
@router.post("/file/upload", response_model = schemas.FileResponse)
def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    file_extension = file.filename.split('.')[-1]
    
    if (file_extension not in ALLOWED_EXTENSIONS):
        print(f"Invalid file type: {file_extension}")
        raise HTTPException(status_code = 400, detail = "Invalid file type. Only CSV and XLSX files are allowed.")

    file_name = file.filename.split('.')[0]
    file_format = file_extension
    
    print(f"Processing file: {file.filename}")

    try:
        db_record = crud.create_file_record(
            db = db, 
            file_name = file_name,
            file_format = file_format
        )
        
        upload_to_minio(file, file.filename)

        return db_record

    except HTTPException as e:
        print(f"HTTP error occurred while uploading file: {e.detail}")
        raise e
    except Exception as e:
        print(f"Error occurred while uploading file: {e}")
        raise HTTPException(status_code = 500, detail = f"An error occurred: {e}")

#  GET Method — View Stored Files  
@router.get("/files", response_model = list[schemas.FileResponse])
def get_all_files(db: Session = Depends(get_db)):
    try:
        records = crud.get_all_file_records(db = db)
        return records
    
    except Exception as e:
        print(f"Error occurred while fetching files: {e}")
        raise HTTPException(status_code = 500, detail = f"An error occurred: {e}")

# GET Method — Merge Two Files Temporarily
@router.get("/files/merge", response_model = schemas.MergeResponse)
async def merge_files(
    file_id_1: int = Query(..., description = "ID of the first file to merge"),
    file_id_2: int = Query(..., description = "ID of the second file to merge"),
    common_column: str = Query(..., description = "Common column to merge on"),
    join_type: str = Query(..., description = "Type of join operation: 'inner', 'outer', 'left', 'right'"),
    db: Session = Depends(get_db),
    cache: InMemoryBackend = Depends(FastAPICache.get_backend)
): 
    try:
        file_record_1 = crud.get_file_record(db = db, file_id = file_id_1)
        file_record_2 = crud.get_file_record(db = db, file_id = file_id_2)

        if not file_record_1 or not file_record_2:
            print(f"One or both file IDs not found: {file_id_1}, {file_id_2}")
            raise HTTPException(status_code = 404, detail = "One or both file IDs not found.")
        
        full_name_1 = f"{file_record_1.file_name}.{file_record_1.file_format}"
        full_name_2 = f"{file_record_2.file_name}.{file_record_2.file_format}"

        df1 = download_from_minio(full_name_1)
        df2 = download_from_minio(full_name_2)

        df1.columns = df1.columns.str.strip().str.lower()
        df2.columns = df2.columns.str.strip().str.lower()

        if common_column not in df1.columns or common_column not in df2.columns:
            print(f"Common column {common_column} not found in one or both files.")
            raise HTTPException(status_code = 400, detail = f"Common column {common_column} not found in one or both files.")

        merged_df = pd.merge(df1, df2, on = common_column, how = join_type)

        if file_record_1.file_format != file_record_2.file_format:
            print(f"File format mismatch: {file_record_1.file_format} vs {file_record_2.file_format}")
            raise HTTPException(status_code = 400, detail = "File format mismatch. Both files must be of the same format to merge.")
        
        if file_record_1.file_format == "csv":
            merged_filename = f"merged_{file_record_1.file_name}_{file_record_2.file_name}_via_{join_type}.csv"
        elif file_record_1.file_format == "xlsx":
            merged_filename = f"merged_{file_record_1.file_name}_{file_record_2.file_name}_via_{join_type}.xlsx"
        else:
            print(f"Unsupported file format: {file_record_1.file_format}")
            raise HTTPException(status_code = 400, detail = "Unsupported file format.")

        cache_key = uuid.uuid4()
        df_json = merged_df.to_json(orient = "records")

        cache_data = {
            "df": df_json,
            "file1_name": full_name_1,
            "file2_name": full_name_2,
            "join_type": join_type,
            "merged_filename": merged_filename
        }

        await cache.set(str(cache_key), json.dumps(cache_data), expire = 300)

        preview_json = merged_df.head().to_dict(orient = "records")

        print(f"Files merged successfully: {full_name_1}, {full_name_2}")

        return schemas.MergeResponse(
            message = "Files merged successfully.",
            cache_key = cache_key,
            preview = preview_json
        )

    except HTTPException as e:
        print(f"HTTP error occurred while merging files: {e.detail}")
        raise e
    except Exception as e:
        print(f"Error occurred while merging files: {e}")
        raise HTTPException(status_code = 500, detail = f"An error occurred: {e}")
    
#  POST Method — Save Merged Dataset Permanently
@router.post("/files/save_merged", response_model = schemas.FileResponse)
async def save_merged_file(
    merged_file: schemas.SaveMergedResponse,
    db: Session = Depends(get_db),
    cache: InMemoryBackend = Depends(FastAPICache.get_backend)
):
    try:
        cache_key = str(merged_file.cache_key)
        cached_json = await cache.get(cache_key)
        
        if not cached_json:
            print(f"Cache key not found or expired: {cache_key}")
            raise HTTPException(status_code = 404, detail = "Cache key not found or expired. Please merge the files again.")
        
        try:
            cache_data = json.loads(cached_json)
        except json.JSONDecodeError:
            print(f"Failed to decode cache data for key: {cache_key}")
            raise HTTPException(status_code = 500, detail = "Failed to decode cached data. Please merge the files again.")

        df_json = cache_data.get("df")
        file1_name = cache_data.get("file1_name")
        file2_name = cache_data.get("file2_name")
        join_type = cache_data.get("join_type")
        merged_filename = cache_data.get("merged_filename")

        if not all([df_json, file1_name, file2_name, join_type, merged_filename]):
            print(f"Incomplete cache data for key: {cache_key}")
            raise HTTPException(status_code = 400, detail = "Incomplete cache data. Please merge the files again.")

        merged_filename = merged_filename
        merged_df = pd.read_json(io.StringIO(df_json), orient = "records")
        file_format = merged_filename.split('.')[-1]

        upload_merged_to_minio(df = merged_df, merged_file_name = merged_filename, file_format = file_format)

        merged_filename_base = merged_filename.split('.')[0]

        db_record = crud.create_file_record(
            db = db,
            file_name = merged_filename_base,
            file_format = file_format
        )

        print(f"Merged file saved successfully: {merged_filename}")

        return db_record

    except HTTPException as e:
        print(f"HTTP error occurred while saving merged file: {e.detail}")
        raise e
    except Exception as e:
        print(f"Error occurred while saving merged file: {e}")
        raise HTTPException(status_code = 500, detail = f"An error occurred: {e}")

