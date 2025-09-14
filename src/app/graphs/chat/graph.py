from typing import Any

from anthropic import AsyncAnthropic
from langgraph.graph import START
from langgraph.graph.state import CompiledStateGraph

from app.graphs.base import GraphBuilder
from app.tools.handler import ToolHandler
from app.utils import PromptStore

from .edges import is_tool_call
from .nodes import LLM, Tool
from .state import ChatGraphState


def create_chat_graph(
    anthropic_client: AsyncAnthropic,
    tool_handler: ToolHandler,
    prompt_store: PromptStore,
    checkpointer: Any = None,
) -> CompiledStateGraph:
    """Create and compile the chat graph.

    Args:
        anthropic_client: The Anthropic client for LLM interactions
        tool_handler: The handler for executing tools

    Returns:
        Compiled chat graph ready for execution
    """
    builder = GraphBuilder(state_schema=ChatGraphState)

    llm_node = LLM(
        anthropic_client=anthropic_client,
        tool_schemas=tool_handler.get_tool_schemas(),
        prompt_store=prompt_store,
    )
    tool_node = Tool(tool_handler=tool_handler)
    builder.add_nodes({"llm": llm_node, "tool": tool_node})

    # Add edges
    builder.add_edges([(START, "llm"), ("tool", "llm")])
    builder.add_conditional_edges(source="llm", condition=is_tool_call)

    return builder.compile(checkpointer=checkpointer)
