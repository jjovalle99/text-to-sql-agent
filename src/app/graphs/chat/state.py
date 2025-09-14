from dataclasses import dataclass, field
from typing import Annotated, Any

from app.models.graph.interrupts import InterruptPolicy


def merge_lists(old: list[Any], new: list[Any]) -> list[Any]:
    """Merge two lists by concatenating them.

    Used for combining message lists in the graph state.

    Args:
        old: Original list to extend
        new: List with new values to append

    Returns:
        Combined list with values from both input lists
    """
    return old + new


@dataclass(slots=True)
class ChatGraphState:
    """State container for the LangGraph workflow.

    Maintains the current state of the graph execution, including
    the message history and the latest stop reason.

    Attributes:
        messages: List of chat messages in the conversation
        stop_reason: Reason why the LLM execution stopped
        interrupt_policy: Policy for interrupting the LLM execution
    """

    messages: Annotated[list[dict[Any, Any]], merge_lists]
    stop_reason: str = field(default_factory=str)
    interrupt_policy: InterruptPolicy = "never"
