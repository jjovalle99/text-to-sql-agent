from typing import Any

from .db import QueryExecutorTool
from .handler import ToolHandler


def get_tool_handler(
    dependencies: dict[str, Any] | None = None,
) -> ToolHandler:
    dependencies = dependencies or {}
    handler = ToolHandler()
    handler.register_tool(QueryExecutorTool(**dependencies))
    return handler
