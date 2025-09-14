from pydantic import BaseModel

from app.models.graph.interrupts import InterruptPolicy


class ChatModelSettings(BaseModel):
    primary_model: str = "claude-sonnet-4-20250514"
    secondary_model: str = "claude-opus-4-1-20250805"
    max_tokens: int = 16_384


class ChatbotRequest(BaseModel):
    content: str
    interrupt_policy: InterruptPolicy = "never"
    tables_schema_xml: str
    chat_model_settings: ChatModelSettings = ChatModelSettings()


class ChatbotResumeRequest(BaseModel):
    query: str
    reason: str | None = None
    tables_schema_xml: str
    chat_model_settings: ChatModelSettings = ChatModelSettings()
