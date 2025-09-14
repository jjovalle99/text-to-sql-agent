from contextlib import asynccontextmanager
from typing import AsyncIterator, TypedDict

import duckdb
import psycopg_pool
from anthropic import AsyncAnthropic
from fastapi import FastAPI
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph.state import CompiledStateGraph
from langsmith.wrappers import wrap_anthropic
from loguru import logger

from app.config import get_settings
from app.db import (
    QueryStore,
    create_db_connection_pool,
)
from app.graphs import create_chat_graph
from app.tools import get_tool_handler
from app.utils import PromptStore


class AppState(TypedDict):
    """Application state container for FastAPI.

    Holds essential resources needed throughout the application lifecycle.

    Attributes:
        db_pool: Connection pool for agent operations
        duck_db_conn: DuckDB connection for in-memory data processing
        graph: Compiled LangGraph workflow for processing queries
        prompt_store: Jinja2 template handler for rendering prompts
        query_store: QueryStore for managing SQL queries
    """

    db_pool: psycopg_pool.AsyncConnectionPool
    duck_db_conn: duckdb.DuckDBPyConnection
    graph: CompiledStateGraph
    prompt_store: PromptStore
    query_store: QueryStore


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[AppState]:
    """Manages the application lifecycle and resources.

    Sets up all necessary resources when the application starts and
    properly cleans them up when the application shuts down. This includes:
    - Database connections
    - External service clients

    Args:
        app: The FastAPI application instance

    Yields:
        Application state containing resources
    """
    settings = get_settings()

    # Database setup
    db_pool = create_db_connection_pool(settings=settings)
    await db_pool.open()
    logger.info("Database pool ready")

    # Initialize external services
    duck_db_conn = duckdb.connect(database=":memory:")
    anthropic_client = wrap_anthropic(client=AsyncAnthropic())
    logger.info("Services initialized")

    # Initialize local services
    tool_handler = get_tool_handler(
        dependencies={
            "conn": duck_db_conn,
        }
    )
    prompt_store = PromptStore(prompts_dir=settings.paths.prompts_dir)
    query_store = QueryStore(base_query_path=settings.paths.queries_dir)

    # Initialize the checkpointer
    checkpointer = AsyncPostgresSaver(conn=db_pool)  # type: ignore

    # Create and compile the graph
    graph = create_chat_graph(
        anthropic_client=anthropic_client,
        tool_handler=tool_handler,
        prompt_store=prompt_store,
        checkpointer=checkpointer,
    )

    yield {
        "db_pool": db_pool,
        "duck_db_conn": duck_db_conn,
        "graph": graph,
        "prompt_store": prompt_store,
        "query_store": query_store,
    }

    # Cleanup
    logger.info("Shutting down services")
    await db_pool.close()
    duck_db_conn.close()
    await anthropic_client.close()
    logger.info("Shutdown complete")
