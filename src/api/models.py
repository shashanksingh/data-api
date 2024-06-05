from pydantic import BaseModel, field_validator, ConfigDict
from typing import List, Literal, Dict

# TODO -> Using AST module/TOML parser populate these constants in CI/CD
TEMPLATES_NAMES = Literal["get_all_submission", "get_all_fuel","get_fte_per_year"]
PARAMS_NAME_LITERAL = {"limit", "sites"}



class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: TEMPLATES_NAMES
    params: Dict[str,str] = None

    @field_validator('params')
    @classmethod
    def params_should_be_from_set(cls,value:Dict) -> Dict:
        if not all({ key in PARAMS_NAME_LITERAL for key,value in value.items() }):
            raise ValueError(f"'{value}' is not a valid parameter name, it should be one of {PARAMS_NAME_LITERAL}")
        return value