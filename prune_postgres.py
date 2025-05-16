#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg==3.2"
# ]
# ///

import psycopg
import logging
import models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    db_conn = get_postgres_connection()
    tables = [
        {
            "table_name": "test_table1",
            "schema": "public",
            "retention_policy": models.DataRetentionPolicy(
                timeretention=models.DataRetentionPolicyTimeRetention(
                    dt_target_column="mtime",
                    retention_seconds=15,
                ),
                conditions=None,
            ),
        }
    ]
    
    for table in tables:
        logger.info("Processing table: %s", table)
        # Create an instance of the PostgresTableModel
        table_model = models.PostgresTableModel(**table)
        # Validate the table model
        if not table_model.validate(db_conn):
            raise ValueError(
                f"Table {table['table_name']} does not exist in schema {table['schema']}"
            )
        
        if table_model.retention_policy:
            logger.info(f"Attempting to prune table {table_model.table_name} with retention policy")
            table_model.prune(db_conn)
            


def get_postgres_connection():
    conn_config = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432,
    }
    return psycopg.connect(**conn_config)



def query_postgres(query):
    conn = get_postgres_connection()
    with conn.cursor() as cur:
        # Example query
        cur.execute(query)
        results = cur.fetchall()

    return results


if __name__ == "__main__":
    logger.info("Starting prune_postgres script")
    try:
        main()
    except Exception as e:
        raise e
    logger.info("Script completed successfully")
