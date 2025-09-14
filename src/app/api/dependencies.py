from typing import Annotated, AsyncIterator

import duckdb
import psycopg
from fastapi import Depends, HTTPException
from fastapi.requests import HTTPConnection
from langgraph.graph.state import CompiledStateGraph
from pydantic import UUID4

from app.models.api.requests import ChatbotRequest, ChatbotResumeRequest
from app.utils.prompts_utils import PromptStore


async def get_db_connection(
    request: HTTPConnection,
) -> AsyncIterator[psycopg.AsyncConnection]:
    async with request.state.db_pool.connection() as conn:
        yield conn


async def get_graph(request: HTTPConnection) -> CompiledStateGraph:
    return request.state.graph


async def get_prompt_store(request: HTTPConnection) -> PromptStore:
    return request.state.prompt_store


async def get_duckdb_connection(
    request: HTTPConnection,
) -> duckdb.DuckDBPyConnection:
    return request.state.duck_db_conn


class ChatRouteDependencies:
    def __init__(
        self,
        user_id: UUID4,
        thread_id: UUID4,
        request: ChatbotRequest,
        graph: Annotated[CompiledStateGraph, Depends(get_graph)],
    ):
        if user_id == thread_id:
            raise HTTPException(
                status_code=400,
                detail="`user_id` cannot be the same as `thread_id`",
            )

        self.user_id = user_id
        self.thread_id = thread_id
        self.request = request
        self.graph = graph


class ResumeRouteDependencies:
    def __init__(
        self,
        user_id: UUID4,
        thread_id: UUID4,
        request: ChatbotResumeRequest,
        graph: Annotated[CompiledStateGraph, Depends(get_graph)],
    ):
        if user_id == thread_id:
            raise HTTPException(
                status_code=400,
                detail="`user_id` cannot be the same as `thread_id`",
            )

        self.user_id = user_id
        self.thread_id = thread_id
        self.request = request
        self.graph = graph
