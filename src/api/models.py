from pydantic import BaseModel


class Filter(BaseModel):
    name: str
    value: str


class QueryRequest(BaseModel):
    query: str
    filter: Filter = None
