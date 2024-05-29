import os

from fastapi import FastAPI, HTTPException
import trino
from api.models import QueryRequest

app = FastAPI()

# Trino configuration
TRINO_HOST = os.environ['TRINO_HOST']
TRINO_PORT = os.environ['TRINO_PORT']
TRINO_USER = os.environ['TRINO_USER']


# Create a Trino connection
def get_trino_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER
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
