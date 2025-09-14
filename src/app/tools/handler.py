from typing import Any

from langgraph.errors import GraphInterrupt
from pydantic import BaseModel, ValidationError
from pydantic.json_schema import (
    GenerateJsonSchema,
    JsonSchemaMode,
    JsonSchemaValue,
)
from pydantic_core import CoreSchema

from .base import BaseTool

type AnyTool = BaseTool[Any, Any, Any]


class NoTitleJsonSchema(GenerateJsonSchema):
    def generate(
        self, schema: CoreSchema, mode: JsonSchemaMode = "validation"
    ) -> JsonSchemaValue:
        json_schema = super().generate(schema, mode=mode)
        json_schema.pop("title", None)
        for property_schema in json_schema.get("properties", {}).values():
            property_schema.pop("title", None)
        return json_schema


class ToolHandler:
    def __init__(self, dependencies: dict[str, Any] | None = None) -> None:
        self.dependencies: dict[str, Any] | None = dependencies or {}
        self._tools: dict[str, AnyTool] = {}
        self._tool_schemas: dict[str, type[BaseModel]] = {}

    def register_tool(self, tool: AnyTool) -> None:
        self._tools[tool.name] = tool
        self._tool_schemas[tool.name] = tool.input_schema

    def get_tool_schemas(self) -> dict[str, dict[str, str | JsonSchemaValue]]:
        return {
            name: {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema.model_json_schema(
                    schema_generator=NoTitleJsonSchema
                ),
            }
            for name, tool in self._tools.items()
        }

    async def execute_tool(
        self, tool_name: str, tool_args: dict[Any, Any], state: Any
    ) -> Any:
        try:
            if tool_name not in self._tools:
                raise ValueError(f"Unknown tool: {tool_name}")

            tool = self._tools[tool_name]
            validated_args = tool.input_schema.model_validate(tool_args)
            return await tool(input_data=validated_args, state=state)
        except ValidationError as e:
            raise ValueError(f"Invalid input for tool '{tool_name}': {e}")
        except GraphInterrupt:
            raise
        except Exception as e:
            raise RuntimeError(
                f"Tool '{tool_name}' execution failed: {e}"
            ) from e
