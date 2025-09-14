from pathlib import Path
from typing import LiteralString, cast

from loguru import logger


class QueryStore:
    def __init__(self, base_query_path: Path) -> None:
        self.base_path = base_query_path
        self._queries: dict[str, str] = {}
        self._load_all_queries()

    def _load_all_queries(self) -> None:
        if not self.base_path.exists():
            raise ValueError(
                f"Query base path does not exist: {self.base_path}"
            )

        for subdir in self.base_path.iterdir():
            if subdir.is_dir() and not subdir.name.startswith("."):
                self._load_queries_from_directory(subdir)

    def _load_queries_from_directory(self, directory: Path) -> None:
        subdir_name = directory.name

        for sql_file in directory.rglob("*.sql"):
            try:
                query_content = sql_file.read_text(encoding="utf-8")

                relative_path = sql_file.relative_to(directory)
                key_parts = list(relative_path.parts)
                key_parts[-1] = key_parts[-1].removesuffix(".sql")

                query_key = f"{subdir_name}.{'.'.join(key_parts)}"
                self._queries[query_key] = query_content

            except Exception as e:
                logger.error(
                    "Failed to load query from {sql_file}: {e}",
                    sql_file=sql_file,
                    e=e,
                )
                raise

    def get_query(self, query_key: str) -> LiteralString:
        query = self._queries.get(query_key)
        if query is None:
            logger.error(
                "Query with key '{query_key}' not found.", query_key=query_key
            )
            raise KeyError(f"Query with key '{query_key}' not found.")
        return cast(LiteralString, query)

    @property
    def available_queries(self) -> list[str]:
        return sorted(self._queries.keys())
