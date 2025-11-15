from sqlalchemy.orm import Session
from models import FileMetadata

def create_file_record(db: Session, file_name: str, file_format: str) -> FileMetadata:

    db_file = FileMetadata(file_name = file_name, file_format = file_format)

    db.add(db_file)
    db.commit()
    db.refresh(db_file)
    
    return db_file

def get_all_file_records(db: Session) -> list[FileMetadata]:

    all_files = db.query(FileMetadata).all()
    return all_files

def get_file_record(db: Session, file_id: int) -> FileMetadata | None:

    file = db.query(FileMetadata).filter(FileMetadata.id == file_id).first()
    return file

