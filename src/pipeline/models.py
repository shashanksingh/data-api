from dataclasses import dataclass


@dataclass
class Table:
    table_name: str
    schema_name: str
    target_table_name: str
    target_schema_name: str

    def __hash__(self):
        return hash(f"{self.schema_name}.{self.table_name}")


@dataclass
class DBCredentials:
    username: str
    password: str
    host: str
    port: int
    database: str
