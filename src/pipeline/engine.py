from typing import Any
import psycopg2 as pg
from sqlalchemy import create_engine
from models import DBCredentials


def get_engine(database: str = "postgres") -> Any:
    # Define the PostgreSQL database connection details
    username = "admin"
    password = "supersecret123"
    host = "localhost"
    port = "5432"

    return pg.connect(
        f"dbname='{database}' user='{username}' host='{host}' port='{port}' password='{password}'"
    )


REPORTING_DATABASE = DBCredentials(
    username="dataapi",
    password="dataapi",
    host="localhost",
    port="5433",
    database="reporting",
)

REPORTING_ENGINE = create_engine(
    f"postgresql+psycopg2://{REPORTING_DATABASE.username}:{REPORTING_DATABASE.password}@{REPORTING_DATABASE.host}:{REPORTING_DATABASE.port}/{REPORTING_DATABASE.database}"
)
