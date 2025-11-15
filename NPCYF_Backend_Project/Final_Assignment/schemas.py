from pydantic import BaseModel, ConfigDict
import uuid

class FileResponse(BaseModel):
    model_config = ConfigDict(from_attributes = True)
    id: int
    file_name: str
    file_format: str

class MergeResponse(BaseModel):
    message: str
    cache_key: uuid.UUID = uuid.uuid4()
    preview: list[dict]

class SaveMergedResponse(BaseModel):
    cache_key: uuid.UUID

