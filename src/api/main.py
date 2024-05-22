from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import trino

app = FastAPI()

# Trino configuration
TRINO_HOST = "trino"
TRINO_PORT = 8080  # default port
TRINO_USER = ""
TRINO_CATALOG = "jdbc"
TRINO_SCHEMA = "jdbc"


# Create a Trino connection
def get_trino_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


class QueryRequest(BaseModel):
    query: str


@app.post("/execute-query/")
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
