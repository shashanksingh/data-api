import os
from fastapi import FastAPI, HTTPException
import trino
from api.models import QueryRequest
from jinja2 import Environment, FileSystemLoader

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


env = Environment(loader=FileSystemLoader("templates"))

def list_templates(template_dir):
    templates = []
    for dirpath, _, filenames in os.walk(template_dir):
        for filename in filenames:
            templates.append(os.path.relpath(os.path.join(dirpath, filename), template_dir))
    return templates


@app.post("/v1/query")
async def execute_query(request: QueryRequest):
    # try:
    conn = get_trino_connection()
    cursor = conn.cursor()

    # template = env.get_template(f"{request.query}.sql.jinja")
    # query = template.render(context=request.filter)
    # cursor.execute(query)

    # result = cursor.fetchall()
    cursor.close()
    return {"result": list_templates("templates")}
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))
