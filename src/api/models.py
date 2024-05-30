from pydantic import BaseModel, field_validator
from typing import Literal


class Filter(BaseModel):
    name: str
    value: str



class QueryRequest(BaseModel):
    query: str
    filter: Filter = None
