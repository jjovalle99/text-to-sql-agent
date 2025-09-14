from pathlib import Path

from pydantic import BaseModel, DirectoryPath


class PathsConfig(BaseModel):
    prompts_dir: DirectoryPath = Path("prompts")
    queries_dir: DirectoryPath = Path("queries")
