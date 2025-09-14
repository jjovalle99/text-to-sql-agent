from typing import Literal

from pydantic import BaseModel

type InterruptPolicy = Literal["never", "final", "always"]


class QueryExecutorHumanFeedback(BaseModel):
    query: str
    reason: str | None = None
