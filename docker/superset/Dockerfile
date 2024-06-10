FROM apache/superset:latest
# Switching to root to install the required packages
USER root

RUN pip install psycopg2-binary
RUN pip install sqlalchemy-trino
RUN pip install --upgrade pip
RUN pip install lunarcalendar tqdm prophet


# Switching back to using the `superset` user
USER superset