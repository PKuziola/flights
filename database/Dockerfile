FROM postgres:11.18-alpine

ENV POSTGRES_PASSWORD postgres
ENV POSTGRES_DB postgres
ENV POSTGRES_USER postgres

COPY flights_db.sql /docker-entrypoint-initdb.d/

EXPOSE 8001