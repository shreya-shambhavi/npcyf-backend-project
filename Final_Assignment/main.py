from fastapi import FastAPI
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from contextlib import asynccontextmanager
from database import engine
import models 
from api import router as api_router

models.Base.metadata.create_all(bind = engine)

@asynccontextmanager
async def lifespan(app: FastAPI):

    FastAPICache.init(InMemoryBackend(), prefix = "fastapi-cache")
    print("Application startup: Cache initialized.")
    
    yield
    
    FastAPICache.clear()
    print("Application shutdown: Cache cleared.")

app = FastAPI(title = "Final Assignment - FastAPI File Management with PostgreSQL, MinIO, and Caching", lifespan = lifespan)

@app.get("/")
def root():
    return {"message": "Welcome to the Final Assignment of NPCYF Backend Project! Please use Swagger for all API interactions at /docs."}

app.include_router(api_router, prefix = "/api/v1")

