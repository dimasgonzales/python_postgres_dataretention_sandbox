#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "fastapi",
#   "uvicorn",
#   "psycopg==3.2",
# ]
# ///
import os
import logging
import psycopg
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, root_validator, Field

# Assuming models.py is in the same directory
import models as domain_models

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# --- Pydantic Models for API ---
class ApiDataRetentionPolicyTimeRetention(BaseModel):
    dt_target_column: str
    retention_seconds: int

class ApiDataRetentionPolicyConditions(BaseModel):
    column: str
    operator: str
    value: str

class ApiDataRetentionPolicy(BaseModel):
    timeretention: Optional[ApiDataRetentionPolicyTimeRetention] = None
    conditions: Optional[List[ApiDataRetentionPolicyConditions]] = None

    @root_validator(pre=True)
    def check_timeretention_or_conditions(cls, values):
        timeretention, conditions = values.get('timeretention'), values.get('conditions')
        if not timeretention and not conditions:
            raise ValueError("Either 'timeretention' or 'conditions' must be provided")
        if timeretention and conditions:
            raise ValueError("Only one of 'timeretention' or 'conditions' can be provided")
        return values

class ApiTableConfig(BaseModel):
    table_name: str = Field(..., examples=["test_table1"])
    schema_name: str = Field(..., examples=["public"]) # Renamed from schema to schema_name to avoid Pydantic conflict
    retention_policy: ApiDataRetentionPolicy

class PruneRequest(BaseModel):
    tables: List[ApiTableConfig]

class PruneResponseDetail(BaseModel):
    table_name: str
    schema_name: str
    status: str
    message: Optional[str] = None

class PruneResponse(BaseModel):
    results: List[PruneResponseDetail]


# --- Database Connection ---
def get_postgres_connection():
    # In a real application, these would come from environment variables or a config file
    conn_config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }
    try:
        conn = psycopg.connect(**conn_config)
        return conn
    except psycopg.OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=503, detail=f"Database connection unavailable: {e}")

@app.get("/health")
async def health_check():
    # Basic health check
    response = {"status": "healthy", "database_accessible": False}
    try:
        conn = get_postgres_connection()
        conn.close()
        response["database_accessible"] = True
    except HTTPException: # Catch exception from get_postgres_connection
        pass # database_accessible remains false
    except Exception as e:
        logger.error(f"Health check database access error: {e}")
        # database_accessible remains false
    return response

@app.post("/prune", response_model=PruneResponse)
async def prune_tables(prune_request: PruneRequest):
    results: List[PruneResponseDetail] = []
    db_conn = None

    try:
        db_conn = get_postgres_connection()
        logger.info(f"Received prune request for {len(prune_request.tables)} table(s)")

        for table_config in prune_request.tables:
            logger.info(f"Processing table: {table_config.schema_name}.{table_config.table_name}")
            
            # Convert Pydantic API models to domain models
            domain_time_retention = None
            if table_config.retention_policy.timeretention:
                domain_time_retention = domain_models.DataRetentionPolicyTimeRetention(
                    dt_target_column=table_config.retention_policy.timeretention.dt_target_column,
                    retention_seconds=table_config.retention_policy.timeretention.retention_seconds
                )

            domain_conditions = None
            if table_config.retention_policy.conditions:
                domain_conditions = [
                    domain_models.DataRetentionPolicyConditions(
                        column=cond.column,
                        operator=cond.operator,
                        value=cond.value
                    ) for cond in table_config.retention_policy.conditions
                ]
            
            domain_retention_policy = domain_models.DataRetentionPolicy(
                timeretention=domain_time_retention,
                conditions=domain_conditions
            )

            table_model = domain_models.PostgresTableModel(
                table_name=table_config.table_name,
                schema=table_config.schema_name, # domain model uses 'schema'
                retention_policy=domain_retention_policy
            )

            try:
                if not table_model.validate(db_conn):
                    message = f"Validation failed for table {table_config.schema_name}.{table_config.table_name}"
                    logger.warning(message)
                    results.append(PruneResponseDetail(
                        table_name=table_config.table_name,
                        schema_name=table_config.schema_name,
                        status="error",
                        message=message
                    ))
                    continue # Skip to next table

                if table_model.retention_policy:
                    logger.info(f"Attempting to prune table {table_model.schema}.{table_model.table_name} with retention policy")
                    table_model.prune(db_conn)
                    message = f"Successfully processed table {table_model.schema}.{table_model.table_name}"
                    logger.info(message)
                    results.append(PruneResponseDetail(
                        table_name=table_config.table_name,
                        schema_name=table_config.schema_name,
                        status="success",
                        message=message
                    ))
                else:
                    # This case should ideally be caught by Pydantic validation if policy is mandatory
                    message = f"No retention policy provided for {table_model.schema}.{table_model.table_name}. Nothing to prune."
                    logger.info(message)
                    results.append(PruneResponseDetail(
                        table_name=table_config.table_name,
                        schema_name=table_config.schema_name,
                        status="skipped",
                        message=message
                    ))

            except ValueError as ve: # Catch validation errors from domain models
                logger.error(f"Error processing table {table_config.schema_name}.{table_config.table_name}: {ve}")
                results.append(PruneResponseDetail(
                    table_name=table_config.table_name,
                    schema_name=table_config.schema_name,
                    status="error",
                    message=str(ve)
                ))
            except Exception as e:
                logger.error(f"Unexpected error processing table {table_config.schema_name}.{table_config.table_name}: {e}", exc_info=True)
                results.append(PruneResponseDetail(
                    table_name=table_config.table_name,
                    schema_name=table_config.schema_name,
                    status="error",
                    message=f"An unexpected error occurred: {e}"
                ))
        
        return PruneResponse(results=results)

    except HTTPException as http_exc: # Re-raise HTTPExceptions (e.g. DB connection)
        raise http_exc
    except Exception as e:
        logger.error(f"General error in /prune endpoint: {e}", exc_info=True)
        # This will be caught by FastAPI's default error handler and return a 500
        raise HTTPException(status_code=500, detail=f"An internal server error occurred: {e}")
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    import uvicorn
    # Ensure the host is 0.0.0.0 to be accessible from outside Docker if running locally for testing
    # and the prune_server is also run locally (not in Docker).
    # If prune_server is in Docker, this allows connections from the host or other containers.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) # Added reload=True for dev
