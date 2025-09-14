import re
import tempfile
import xml.etree.ElementTree as ET
from typing import Annotated, Any

import duckdb
from fastapi import APIRouter, Depends, UploadFile, status
from fastapi.encoders import jsonable_encoder
from loguru import logger
from nanoid import generate

from ..dependencies import get_duckdb_connection

router = APIRouter(tags=["data"])


def make_safe_table_name(raw: str) -> str:
    base = raw.removesuffix(".csv")
    safe = re.sub(r"[^A-Za-z0-9_]", "_", base)
    final_name = f"{generate(alphabet='_abcdefghijklmnopqrst', size=4).lower()}_{safe.lower()}"
    return final_name


def schemas_to_xml_str(payload: dict[str, Any]) -> str:
    root = ET.Element("tables_schema")
    for tbl in payload.get("schemas", []):
        table_el = ET.SubElement(root, "table", {"name": tbl["table_name"]})
        for col in tbl.get("schema", []):
            ((col_name, data_type),) = col.items()
            ET.SubElement(
                table_el, "column", {"name": col_name, "data_type": data_type}
            )

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    return ET.tostring(root, encoding="unicode")


@router.post("/upload", status_code=status.HTTP_200_OK)
async def upload_dataset(
    files: list[UploadFile],
    duck_db_conn: Annotated[
        duckdb.DuckDBPyConnection, Depends(get_duckdb_connection)
    ],
) -> Any:
    schemas = []
    for file in files:
        table_name = make_safe_table_name(
            raw=file.filename or "uploaded_file.csv"
        )

        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".csv", delete=True
        ) as tmp:
            await file.seek(0)
            chunk_size = 1024 * 1024
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                tmp.write(chunk)

            tmp.flush()
            duck_db_conn.execute(
                f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_csv_auto(?)
                """,
                [tmp.name],
            )

        cols = duck_db_conn.execute(
            f'PRAGMA table_info("{table_name}")'
        ).fetchall()
        schema = [{c[1]: c[2]} for c in cols]
        schemas.append({"table_name": table_name, "schema": schema})

    return jsonable_encoder(
        {
            "status": "success",
            "tables": [file.filename for file in files],
            "tables_schema_xml": schemas_to_xml_str({"schemas": schemas}),
        }
    )


@router.get("/tables")
async def list_tables(
    duck_db_conn: Annotated[
        duckdb.DuckDBPyConnection, Depends(get_duckdb_connection)
    ],
):
    tables_info = duck_db_conn.execute("""
        SELECT 
            table_name,
            estimated_size,
            column_count
        FROM duckdb_tables()
        WHERE NOT starts_with(table_name, 'duckdb_')
        ORDER BY estimated_size DESC
    """).fetchall()

    tables = []
    for table in tables_info:
        tables.append(
            {
                "name": table[0],
                "rows": table[1],
                "columns": table[2],
            }
        )
    return jsonable_encoder(
        {
            "tables": tables,
            "total_tables": len(tables),
        }
    )


@router.delete("/tables/{table_name}")
async def drop_table(
    table_name: str,
    duck_db_conn: Annotated[
        duckdb.DuckDBPyConnection, Depends(get_duckdb_connection)
    ],
):
    try:
        duck_db_conn.execute(f"DROP TABLE {table_name}")

        return {
            "status": "success",
            "message": f"Table '{table_name}' dropped successfully",
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.delete("/tables")
async def clear_all_tables(
    duck_db_conn: Annotated[
        duckdb.DuckDBPyConnection, Depends(get_duckdb_connection)
    ],
):
    tables = duck_db_conn.execute("SHOW TABLES").fetchall()

    dropped = []
    for (table_name,) in tables:
        try:
            duck_db_conn.execute(f"DROP TABLE {table_name}")
            dropped.append(table_name)
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}")
            pass

    return {
        "status": "success",
        "dropped_tables": dropped,
        "count": len(dropped),
    }
