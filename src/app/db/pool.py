import psycopg_pool

from app.config import Settings


def create_db_connection_pool(
    settings: Settings,
) -> psycopg_pool.AsyncConnectionPool:
    return psycopg_pool.AsyncConnectionPool(
        conninfo=settings.db.connection_string,
        open=False,
        min_size=0,
        max_size=3,
    )
