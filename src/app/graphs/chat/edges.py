from typing import Literal

from .state import ChatGraphState


def is_tool_call(state: ChatGraphState) -> Literal["tool", "__end__"]:
    """Determine whether to route to tool node or to finish the execution.

    Args:
        state: Current graph state.

    Returns:
        Name of the next node to route to ("tool" or "__end__").
    """
    if state.stop_reason == "tool_use":
        return "tool"
    else:
        return "__end__"
