from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import lifespan
from app.api.routers import chat_router, data_router, health_router

app = FastAPI(
    title="Text-to-SQL Agent",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

api_router = APIRouter(prefix="/v1/api")
api_router.include_router(router=health_router, prefix="/health")
api_router.include_router(router=chat_router)
api_router.include_router(router=data_router, prefix="/data")
app.include_router(router=api_router)
