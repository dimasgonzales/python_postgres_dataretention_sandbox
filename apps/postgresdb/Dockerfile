# filepath: /Users/dimas/source/python_postgres_dataretention_sandbox/apps/postgresdb/Dockerfile
FROM postgres:15-bookworm

# Install pg_partman
RUN apt-get update && \
    apt-get install -y postgresql-15-partman

# Create pg_partman extension in the database
COPY ./init.sql /docker-entrypoint-initdb.d/

# Expose the PostgreSQL port
EXPOSE 5432