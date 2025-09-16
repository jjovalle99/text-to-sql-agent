<h1 align="center">Text-to-SQL-Agent Backend</h1>
<div align="center">
    <a align="center" href="https://www.python.org/downloads/release/python-3128/"><img src="https://img.shields.io/badge/python-3.12.8-red"/></a>
    <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-0.116.1-009688.svg?style=flat&logo=FastAPI&logoColor=white"/></a>
    <a href="https://github.com/astral-sh/uv"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json"/></a>
    <a href="http://mypy-lang.org/"><img src="http://www.mypy-lang.org/static/mypy_badge.svg"/></a>
    <a href="https://github.com/astral-sh/ruff"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json" alt="Ruff" style="max-width:100%;"></a>
</div>

## Description
This repository contains the backend implementation of a Text2SQL agent using LangGraph and Anthropic's Claude 4 Sonnet. The agent is designed to interpret natural language queries and convert them into SQL queries that can be executed against a database. We use extended thinking and interleaved thinking which enables the agent to reason and reflect on its queries and actions. 

Ideally, this agent will be connected to a enterprise data warehouse with read-only access. However, for demonstration purposes we can not expose any company data. Therefore, we enable users that want to test the agent to use data in .csv files. You can use as many .csv files as you want. The agent will read the files, infer the schema and persist them in a DuckDB database.

You can check the demo at [https://sql-chatbot-nu.vercel.app/](https://sql-chatbot-nu.vercel.app/).

**If you are interested in the frontend code, please visit the following repository: [https://github.com/jovalle02/sql-chatbot](https://github.com/jovalle02/sql-chatbot)**

## Stack
* Programming Language: [Python 3.12.8](https://www.python.org/)
* LLM Framework: [LangGraph](https://www.langchain.com/langgraph)
* LLM Provider: [Anthropic](https://www.anthropic.com/)
* Database: [PostgreSQL](https://www.postgresql.org/)
* SQL Tool Calling: [DuckDB](https://duckdb.org/)
* Framework: [FastAPI 0.116.1](https://fastapi.tiangolo.com/)
* Dependency & Package Manager: [uv](https://docs.astral.sh/uv/)
* Linters: [Ruff](https://docs.astral.sh/ruff/)
* Type Checking: [MyPy](https://mypy-lang.org/)
* Deployment: [Docker](https://www.docker.com/)

## Configure

1. **Clone the Git repository to your local machine:**
```shell
git clone https://github.com/jjovalle99/text-to-sql-agent.git
cd text-to-sql-agent
```

2. **Copy the stub `.env copy` file to `.env` and replace placeholder values with the required credentials**:
```shell
cp .env\ copy .env
```
```
# PostgreSQL
DB__HOST=
DB__PORT=
DB__DATABASE=
DB__USER=
DB__PASSWORD=
DB__SCHEMA_NAME=

# LangSmith
LANGSMITH_TRACING=
LANGSMITH_ENDPOINT=
LANGSMITH_API_KEY=
LANGSMITH_PROJECT=

# Anthropic
ANTHROPIC_API_KEY=
ANTHROPIC_API_KEY=fillme
```

3. **Create the database schema and checkpointer tables**
```shell
uv run python scripts/initialize_db.py
```

## Installation and Usage

There are two ways to run the application: using Docker or running it locally in the shell.

### Using `Docker`

1. **Ensure Docker is installed on your machine.** For more information, visit the official [Docker documentation](https://docs.docker.com/).

2. **Build the Docker image:**

   ```shell
   make docker_build
   ```
   **Note**: The docker image is about 457Mb in size.

3. **Run the Docker container:**

   ```shell
    make docker_run
    ```
    **Note:** This will work only if you have the required credentials in the `.env` file.    
    **Note:** You can inspect the running container by running `make docker_logs`.

4. **Access the application docs.** Visit the app docs at [http://localhost:8000/docs](http://localhost:8000/docs). If everything is working correctly, you'll see the UI.

5. **Stop the Docker container:**

   ```shell
   make docker_stop
   ```

### Running Locally in the Shell

1. **Install `uv` by following the instructions** [here](https://docs.astral.sh/uv/getting-started/installation/).

2. **Install the dependencies and package:**

```shell
uv sync --all-groups
```
3. **Start the API:**

   ```shell
   make dev
   ```
    **Note:** This will work only if you have the required credentials in the `.env` file.

4. **Access the application and database.** Visit the default app path at [http://localhost:8000](http://localhost:8000). If everything is working correctly, you'll see the UI.

6. **Stop the application and background services.** Terminate the processes you (this may involve using `Ctrl+C` in the terminal)
    
## Structure
```shell
├── README.md
├── Dockerfile
├── Makefile
├── pyproject.toml
├── uv.lock
├── prompts                        // Jinja2 templates for prompts
│   └── system.jinja2
├── queries                        // SQL queries for database interactions
│   └── ddl
│       ├── create_checkpoint_index.sql
│       └── create_schema.sql
├── scripts                        // Database initialization scripts
│   └── initialize_db.py
├── src
│   └── app                        // Main application package
│       ├── __init__.py
│       ├── api                    // API definitions and routes
│       │   ├── __init__.py
│       │   ├── dependencies.py
│       │   ├── lifespan.py
│       │   └── routers
│       │       ├── __init__.py
│       │       ├── chat.py
│       │       ├── data.py
│       │       └── health.py
│       ├── config                 // Application settings and configuration
│       │   ├── __init__.py
│       │   ├── database.py
│       │   ├── paths.py
│       │   └── settings.py
│       ├── db                     // Database connection handling
│       │   ├── __init__.py
│       │   ├── pool.py
│       │   └── query_store.py
│       ├── graphs                 // LangGraph graph definition
│       │   ├── __init__.py
│       │   ├── base
│       │   │   ├── __init__.py
│       │   │   ├── builder.py
│       │   │   └── node.py
│       │   └── chat
│       │       ├── __init__.py
│       │       ├── edges.py
│       │       ├── graph.py
│       │       ├── nodes
│       │       │   ├── __init__.py
│       │       │   ├── llm.py
│       │       │   └── tool.py
│       │       └── state.py
│       ├── models                 // Data models and schemas
│       │   ├── __init__.py
│       │   ├── api
│       │   │   ├── __init__.py
│       │   │   └── requests
│       │   │       ├── __init__.py
│       │   │       └── chat.py
│       │   └── graph
│       │       ├── __init__.py
│       │       └── interrupts.py
│       ├── tools                  // Tool implementations for LLM
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── db.py
│       │   └── handler.py
│       └── utils                  // Utility functions and helpers
│           ├── __init__.py
│           └── prompts_utils.py
└── server.py                      // Entry point to start the application
```