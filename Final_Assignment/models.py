from sqlalchemy import Column, Integer, String
from database import Base

class FileMetadata(Base):
    
    __tablename__ = "file_metadata"

    id = Column(Integer, primary_key = True, index = True)
    file_name = Column(String, index = True)
    file_format = Column(String(5))

