from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template


class PromptStore:
    def __init__(self, prompts_dir: Path) -> None:
        self.environment = Environment(
            loader=FileSystemLoader(searchpath=str(prompts_dir)),
            autoescape=True,
        )

    def get_prompt(self, prompt_name: str) -> Template:
        return self.environment.get_template(prompt_name)

    @property
    def available_prompts(self) -> list[str]:
        return self.environment.list_templates(extensions=["jinja2"])
