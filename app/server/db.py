import os
from databricks import sql as databricks_sql
from .config import get_workspace_client, WAREHOUSE_ID, IS_DATABRICKS_APP


def get_connection():
    w = get_workspace_client()
    host = os.environ.get("DATABRICKS_HOST", "") if IS_DATABRICKS_APP else w.config.host
    if host.startswith("https://"):
        host = host[8:]
    if host.startswith("http://"):
        host = host[7:]

    if IS_DATABRICKS_APP:
        token = w.config.authenticate()
        access_token = token.get("Authorization", "").replace("Bearer ", "") if isinstance(token, dict) else None
        if not access_token:
            access_token = os.environ.get("DATABRICKS_TOKEN", "")
    else:
        token = w.config.authenticate()
        access_token = token.get("Authorization", "").replace("Bearer ", "") if isinstance(token, dict) else ""

    return databricks_sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=access_token,
    )


def execute_query(query: str, params=None):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()
