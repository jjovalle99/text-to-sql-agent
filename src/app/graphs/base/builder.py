from typing import Any, Callable

from langgraph.graph import StateGraph
from langgraph.graph.state import CompiledStateGraph

from .node import Node


class GraphBuilder[StateType: Any]:
    """Builder for creating and configuring LangGraph state graphs.

    Provides a simplified interface for constructing LangGraph workflows
    by handling node registration, edge connections, and conditional routing.

    Attributes:
        state_schema: Schema defining the structure of the graph state
        graph: The underlying StateGraph instance being built
    """

    def __init__(
        self,
        state_schema: type[StateType],
        context_schema: Any | None = None,
    ) -> None:
        """Initialize a new graph builder.

        Args:
            state_schema: Schema defining the structure of the graph state
            context_schema: Optional schema for context
        """
        self.state_schema = state_schema
        self.graph = StateGraph(
            state_schema=state_schema, context_schema=context_schema
        )

    def add_nodes(self, nodes: dict[str, Node[StateType]]) -> None:
        """Register processing nodes in the graph.

        Args:
            nodes: Dictionary mapping node names to node implementations
        """
        for name, action in nodes.items():
            self.graph.add_node(node=name, action=action)  # pyright: ignore

    def add_edges(self, edges: list[tuple[str, str]]) -> None:
        """Connect nodes with directed edges.

        Args:
            edges: List of (source, target) node name tuples
        """
        for source, target in edges:
            self.graph.add_edge(start_key=source, end_key=target)

    def add_conditional_edges(
        self, source: str, condition: Callable[[StateType], str]
    ) -> None:
        """Add conditional routing from a source node.

        Args:
            source: Name of the source node
            condition: Function that determines the next node based on state
        """
        self.graph.add_conditional_edges(source=source, path=condition)

    def compile(
        self, debug: bool = False, checkpointer: Any = None
    ) -> CompiledStateGraph:
        """Compile the graph into an executable form.

        Args:
            debug: Whether to enable debug mode
            checkpointer: Optional checkpointer for saving state during execution

        Returns:
            Compiled graph ready for execution
        """
        return self.graph.compile(debug=debug, checkpointer=checkpointer)  # pyright: ignore
