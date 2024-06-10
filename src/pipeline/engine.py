import os
from typing import Any
import psycopg2 as pg
from sqlalchemy import create_engine
from models import DBCredentials

# Trino configuration
TRANSACTIONAL_POSTGRES_USERNAME = os.environ["TRANSACTIONAL_POSTGRES_USERNAME"]
TRANSACTIONAL_POSTGRES_HOSTNAME = os.environ["TRANSACTIONAL_POSTGRES_HOSTNAME"]
TRANSACTIONAL_POSTGRES_PORT = os.environ["TRANSACTIONAL_POSTGRES_PORT"]
TRANSACTIONAL_POSTGRES_PASSWORD = os.environ["TRANSACTIONAL_POSTGRES_PASSWORD"]
REPORTING_POSTGRES_USERNAME = os.environ["REPORTING_POSTGRES_USERNAME"]
REPORTING_POSTGRES_PASSWORD = os.environ["REPORTING_POSTGRES_PASSWORD"]
REPORTING_POSTGRES_HOSTNAME = os.environ["REPORTING_POSTGRES_HOSTNAME"]
REPORTING_POSTGRES_PORT = os.environ["REPORTING_POSTGRES_PORT"]
REPORTING_POSTGRES_DATABASE = os.environ["REPORTING_POSTGRES_DATABASE"]


def get_engine(database: str = "postgres") -> Any:
    # Define the PostgreSQL database connection details
    username = "admin"
    password = "supersecret123"
    host = "localhost"
    port = "5432"

    return pg.connect(
        f"dbname='{database}' user='{username}' host='{host}' port='{port}' password='{password}'"
    )


REPORTING_ENGINE = create_engine(
    f"postgresql+psycopg2://{REPORTING_POSTGRES_USERNAME}:{REPORTING_POSTGRES_PASSWORD}@{REPORTING_POSTGRES_HOSTNAME}:{REPORTING_POSTGRES_PORT}/{REPORTING_POSTGRES_DATABASE}"
)
