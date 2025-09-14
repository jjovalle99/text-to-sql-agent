from typing import Any

from langchain_core.runnables.config import RunnableConfig
from langgraph.errors import GraphInterrupt
from langgraph.types import StreamWriter

from app.graphs import Node
from app.tools.handler import ToolHandler

from ..state import ChatGraphState


class Tool(Node[ChatGraphState]):
    """Executes tools requested by the LLM.

    Attributes:
        name: Identifier for the node in the graph.
    """

    name = "tool"

    def __init__(
        self,
        tool_handler: ToolHandler,
    ) -> None:
        self.tool_handler = tool_handler

    async def __call__(
        self,
        state: ChatGraphState,
        config: RunnableConfig,
        writer: StreamWriter,
    ) -> ChatGraphState:
        """Executes tools based on the last message in the graph state.

        Args:
            state: Current state of the graph
            config: Configuration for the node execution
            writer: Stream writer for streaming custom events

        Returns:
            Updated graph state with tool message and the llm stop reason.
        """
        last_message = state.messages[-1]
        output_messages: list[dict[str, Any]] = []

        found_text_content: bool = False
        for block in last_message["content"]:
            if not found_text_content and block["type"] == "text":
                found_text_content = True

            if block["type"] == "tool_use":
                writer({"block-start": "tool-output"})
                try:
                    tool_result = await self.tool_handler.execute_tool(
                        tool_name=block["name"],
                        tool_args=block["input"],
                        state=state,
                    )
                except GraphInterrupt:
                    raise
                except Exception as e:
                    tool_result = f"Error executing tool: {e}"
                writer({"tool-output": tool_result})
                writer({"block-end": "tool-output"})

                message: dict[str, Any] = {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": block["id"],
                            "content": tool_result,
                        }
                    ],
                }
                output_messages.append(message)

        if found_text_content:
            writer(" ")

        return ChatGraphState(
            messages=output_messages,
            stop_reason=state.stop_reason,
            interrupt_policy=state.interrupt_policy,
        )
