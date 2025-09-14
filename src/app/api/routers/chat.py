import json
from typing import Annotated, Any, AsyncIterator

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Command

from ..dependencies import ChatRouteDependencies, ResumeRouteDependencies

router = APIRouter(tags=["chat"])


def format_sse_event(event_type: str, data: dict[str, Any]) -> str:
    """Format data as a Server-Sent Event (SSE) string.

    Args:
        event_type: Type of the SSE event
        data: Data to include in the event

    Returns:
        Formatted SSE event string
    """
    return (
        f"event: {event_type}\ndata: {json.dumps(jsonable_encoder(data))}\n\n"
    )


async def _execute_stream(
    graph: CompiledStateGraph,
    input: Any,
    config: dict[str, Any],
) -> AsyncIterator[str]:
    try:
        async for mode, event in graph.astream(
            input=input,
            config=config,  # type: ignore
            stream_mode=["custom", "updates"],
        ):
            if mode == "updates" and "__interrupt__" not in event:
                continue

            for event_type, event_data in event.items():  # type: ignore
                yield format_sse_event(event_type=event_type, data=event_data)

        yield format_sse_event(
            event_type="complete",
            data={"status": "success"},
        )
    except Exception as e:
        error_payload = {"status": "error", "error": str(e)}
        yield format_sse_event(event_type="error", data=error_payload)


@router.post(
    "/stream/{user_id}/{thread_id}",
    status_code=200,
)
async def stream_chatbot_response(
    chat_dependencies: Annotated[
        ChatRouteDependencies, Depends(ChatRouteDependencies)
    ],
) -> StreamingResponse:
    input = {
        "messages": [
            {"role": "user", "content": chat_dependencies.request.content}
        ],
        "interrupt_policy": chat_dependencies.request.interrupt_policy,
    }
    config = {
        "configurable": {
            "llm": {
                **chat_dependencies.request.chat_model_settings.model_dump(),
                "tables": chat_dependencies.request.tables_schema_xml,
            },
            "thread_id": str(chat_dependencies.thread_id),
        },
        "metadata": {"user_id": str(chat_dependencies.user_id)},
    }

    return StreamingResponse(
        content=_execute_stream(
            graph=chat_dependencies.graph,
            input=input,
            config=config,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post(
    "/stream/{user_id}/{thread_id}/resume",
    status_code=200,
)
async def resume_chatbot_response(
    resume_dependencies: Annotated[
        ResumeRouteDependencies, Depends(ResumeRouteDependencies)
    ],
) -> StreamingResponse:
    input: Any = Command(
        resume={
            "query": resume_dependencies.request.query,
            "reason": resume_dependencies.request.reason,
        }
    )
    config = {
        "configurable": {
            "llm": resume_dependencies.request.chat_model_settings.model_dump(),
            "thread_id": str(resume_dependencies.thread_id),
        },
        "metadata": {"user_id": str(resume_dependencies.user_id)},
    }

    return StreamingResponse(
        content=_execute_stream(
            graph=resume_dependencies.graph,
            input=input,
            config=config,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
