from abc import ABC, abstractmethod
from typing import ClassVar

from langchain_core.runnables.config import RunnableConfig
from langgraph.types import StreamWriter


class Node[StateType](ABC):
    """Base class for all processing nodes in the LangGraph workflow.

    Defines the common interface that all nodes must implement.
    Each node processes the graph state and returns an updated state.

    Attributes:
        name: Class variable defining the node's identifier in the graph
    """

    name: ClassVar[str]

    @abstractmethod
    async def __call__(
        self, state: StateType, config: RunnableConfig, writer: StreamWriter
    ) -> StateType:
        """Process the current graph state and return updated state.

        Args:
            state: Current state of the graph
            config: Configuration for the node execution
            writer: Stream writer for streaming custom events

        Returns:
            Updated graph state after processing
        """
        pass
