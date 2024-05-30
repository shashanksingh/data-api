FROM postgres:16
WORKDIR /data
#COPY data/init.sql /docker-entrypoint-initdb.d/
RUN echo "create database reporting" >> /docker-entrypoint-initdb.d/init.sql