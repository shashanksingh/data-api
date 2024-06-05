import asyncio
import os
from typing import List

from fastapi import FastAPI, HTTPException
import trino
from api.models import QueryRequest
from jinja2 import Environment, FileSystemLoader

app = FastAPI()

# Trino configuration
TRINO_HOST = os.environ["TRINO_HOST"]
TRINO_PORT = os.environ["TRINO_PORT"]
TRINO_USER = os.environ["TRINO_USER"]


# Create a Trino connection
def get_trino_connection():
    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)


# async def run_query_async(query):
#     return await asyncio.to_thread(execute_query, query)


env = Environment(loader=FileSystemLoader("./api/templates"))


async def get_result_for_query(query: QueryRequest, trino_connection: trino.dbapi.Connection) -> List[str]:
    # trino_connection=get_trino_connection()
    cursor = trino_connection.cursor()
    template = env.get_template(f"{query.query}.sql.jinja")
    query = template.render(context=query.params) if hasattr(query, 'params') else template.render()
    cursor.execute(query)
    return cursor.fetchall()


@app.post("/v1/query")
async def execute_query(request: QueryRequest):
    # try:
    conn = get_trino_connection()
    cursor = conn.cursor()

    result = await get_result_for_query(query=request, trino_connection=conn)
    cursor.close()
    return {"result": result}
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))


@app.post("/v1/queries")
async def execute_query(requests: List[QueryRequest]):
    # try:
    conn = get_trino_connection()
    cursor = conn.cursor()

    tasks = [get_result_for_query(query=request, trino_connection=conn) for request in requests]
    results = await asyncio.gather(*tasks)

    cursor.close()
    return {"results": results}
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))
