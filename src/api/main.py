from fastapi import FastAPI, HTTPException
import trino
from src.api.models import QueryRequest

app = FastAPI()

# Trino configuration
TRINO_HOST = "trino"
TRINO_PORT = 8080  # default port
TRINO_USER = ""


# Create a Trino connection
def get_trino_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


@app.post("/v1/query")
async def execute_query(request: QueryRequest):
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        cursor.execute(request.query)
        result = cursor.fetchall()
        cursor.close()
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
