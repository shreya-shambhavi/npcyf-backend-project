# Final Assignment - FastAPI File Management with PostgreSQL, MinIO, and Caching

## 1. Project Structure

    final-assignment/
        ├── .gitignore
        ├── .env
        ├── requirements.txt
        ├── main.py
        ├── config.py
        ├── database.py
        ├── models.py
        ├── schemas.py
        ├── crud.py
        ├── minio_client.py
        └── api.py

## 2. Set up Virtual Environment

    python -m venv venv

    .\venv\Scripts\activate

    pip install -r requirements.txt

## 3. Set up MinIO Server

    # First and foremost install minio on the system.

    cd C:\minio\final-assignment-data  
    
    .\minio.exe server C:\minio\final-assignment-data --console-address ":9001" 

## 4. Run the Uvicorn Server

    uvicorn main:app --reload

