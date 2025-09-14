from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class BaseTool[StateType: Any, InputType: BaseModel, OutputType](
    ABC, BaseModel
):
    name: str
    description: str
    input_schema: type[InputType]

    @abstractmethod
    async def __call__(
        self, input_data: InputType, state: StateType
    ) -> OutputType:
        pass
