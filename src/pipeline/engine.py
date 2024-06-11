import os
from typing import Any
import psycopg2 as pg
from sqlalchemy import create_engine
from models import DBCredentials

# Postgres configuration
# TRANSACTIONAL_POSTGRES_USERNAME = os.environ["TRANSACTIONAL_POSTGRES_USERNAME"]
# TRANSACTIONAL_POSTGRES_HOSTNAME = os.environ["TRANSACTIONAL_POSTGRES_HOSTNAME"]
# TRANSACTIONAL_POSTGRES_PORT = os.environ["TRANSACTIONAL_POSTGRES_PORT"]
# TRANSACTIONAL_POSTGRES_PASSWORD = os.environ["TRANSACTIONAL_POSTGRES_PASSWORD"]
# TRANSACTIONAL_POSTGRES_DATABASE = os.environ["TRANSACTIONAL_POSTGRES_DATABASE"]
# REPORTING_POSTGRES_USERNAME = os.environ["REPORTING_POSTGRES_USERNAME"]
# REPORTING_POSTGRES_PASSWORD = os.environ["REPORTING_POSTGRES_PASSWORD"]
# REPORTING_POSTGRES_HOSTNAME = os.environ["REPORTING_POSTGRES_HOSTNAME"]
# REPORTING_POSTGRES_PORT = os.environ["REPORTING_POSTGRES_PORT"]
# REPORTING_POSTGRES_DATABASE = os.environ["REPORTING_POSTGRES_DATABASE"]

TRANSACTIONAL_POSTGRES_USERNAME = os.getenv("TRANSACTIONAL_POSTGRES_USERNAME", "admin")
TRANSACTIONAL_POSTGRES_HOSTNAME = os.getenv(
    "TRANSACTIONAL_POSTGRES_HOSTNAME", "localhost"
)
TRANSACTIONAL_POSTGRES_PORT = os.getenv("TRANSACTIONAL_POSTGRES_PORT", "5432")
TRANSACTIONAL_POSTGRES_PASSWORD = os.getenv(
    "TRANSACTIONAL_POSTGRES_PASSWORD", "supersecret123"
)
TRANSACTIONAL_POSTGRES_DATABASE = os.getenv(
    "TRANSACTIONAL_POSTGRES_DATABASE", "postgres"
)
REPORTING_POSTGRES_USERNAME = os.getenv("REPORTING_POSTGRES_USERNAME", "dataapi")
REPORTING_POSTGRES_PASSWORD = os.getenv("REPORTING_POSTGRES_PASSWORD", "dataapi")
REPORTING_POSTGRES_HOSTNAME = os.getenv("REPORTING_POSTGRES_HOSTNAME", "localhost")
REPORTING_POSTGRES_PORT = os.getenv("REPORTING_POSTGRES_PORT", "5433")
REPORTING_POSTGRES_DATABASE = os.getenv("REPORTING_POSTGRES_DATABASE", "reporting")


def get_engine(database: str = "postgres") -> Any:
    # Define the PostgreSQL database connection details

    return pg.connect(
        f"dbname='{TRANSACTIONAL_POSTGRES_DATABASE}' user='{TRANSACTIONAL_POSTGRES_USERNAME}' host='{TRANSACTIONAL_POSTGRES_HOSTNAME}' port='{TRANSACTIONAL_POSTGRES_PORT}' password='{TRANSACTIONAL_POSTGRES_PASSWORD}'"
    )


REPORTING_ENGINE = create_engine(
    f"postgresql+psycopg2://{REPORTING_POSTGRES_USERNAME}:{REPORTING_POSTGRES_PASSWORD}@{REPORTING_POSTGRES_HOSTNAME}:{REPORTING_POSTGRES_PORT}/{REPORTING_POSTGRES_DATABASE}"
)
