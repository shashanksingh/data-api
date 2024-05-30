from pydantic import BaseModel, field_validator, ConfigDict
from typing import List


class Filter(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: List[str]


class Param(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    value: str


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str
    params: List[Param] = None
    filter: Filter = None
