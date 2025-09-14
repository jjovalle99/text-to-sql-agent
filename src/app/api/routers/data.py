import re
import tempfile
import xml.etree.ElementTree as ET
from typing import Annotated, Any

import duckdb
from fastapi import APIRouter, Depends, UploadFile, status
from fastapi.encoders import jsonable_encoder
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
