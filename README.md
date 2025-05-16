# Python Postgres Data Retention Sandbox

This repository demonstrates a proof-of-concept (POC) for implementing time-based data retention policies in PostgreSQL. It utilizes Docker, the `pg_partman` extension for table partitioning, and custom Python scripts for data pruning.

## Overview

The project showcases how to:
*   Set up a PostgreSQL database using Docker.
*   Employ `pg_partman` to automatically partition a table (`test_table1`) based on a timestamp column (`mtime`).
*   Continuously insert data into the partitioned table.
*   Apply a data retention policy using a Python script (`prune_postgres.py`) that removes old data partitions.

## How to Run

**Prerequisites:**
*   Docker
*   Docker Compose (usually included with Docker Desktop)
*   A shell environment (like bash)

**Command:**
```bash
bash start_demo.sh
```

**What Happens:**
1.  The script initializes a PostgreSQL 15 container using Docker Compose. The `pg_partman` extension is enabled.
2.  The `test_table1` is created and configured for time-based partitioning by `pg_partman` (new partitions are created for each second of data based on the `mtime` column).
3.  For 60 seconds, sample data is continuously inserted into `test_table1`. `pg_partman`'s maintenance is run to create new partitions as needed.
4.  After data insertion, the `prune_postgres.py` script is executed. This script applies a 15-second retention policy:
    *   It identifies and drops data partitions older than 15 seconds.
    *   **Crucially, it then drops the main `test_table1` parent table itself.**

You will see log output detailing these stages, including data insertion, partition management, and the pruning process.

## Core Mechanism

*   **Partitioning with `pg_partman`:** The `test_table1` is partitioned by its `mtime` (timestamp) column. `pg_partman` automatically creates new child tables (partitions) for incoming data, with each partition in this demo covering a 1-second interval. This allows for efficient deletion of old data by dropping entire partitions.
*   **Pruning Logic (`prune_postgres.py` & `models.py`):**
    *   The Python script defines a retention policy (e.g., keep data for the last 15 seconds).
    *   It inspects the names of the partitions (e.g., `test_table1_p20231026_143000`) to determine the time window they represent.
    *   Partitions containing data older than the retention period are dropped.
    *   As mentioned, the current demo implementation also drops the parent `test_table1` after pruning its old partitions.

## Key Files

*   **`start_demo.sh`**: The main script to set up, run, and demonstrate the data retention process.
*   **`docker-compose.yml`**: Defines the PostgreSQL Docker service.
*   **`apps/postgresdb/Dockerfile`**: Customizes the `postgres` image to include `pg_partman`.
*   **`apps/postgresdb/init.sql`**: SQL script run on container startup to enable the `pg_partman` extension.
*   **`scripts/initialize_postgres.sql`**: SQL script to create `test_table1` and configure `pg_partman` for it.
*   **`scripts/insert_data_into_testtable1.sql`**: SQL script to insert sample data.
*   **`models.py`**: Python dataclasses defining retention policies and the `PostgresTableModel` for managing table pruning.
*   **`prune_postgres.py`**: Python script that applies the defined retention policy to `test_table1`.

## Limitations & Notes

*   **Parent Table Deletion:** The current pruning logic in `models.py` (specifically the `_drop_table` method) is designed for this demo and **drops the parent table (`test_table1`)** after removing its old partitions. In a production scenario, you would likely want to keep the parent table.
*   **Condition-Based Retention:** The framework in `models.py` includes structures for condition-based retention (e.g., `DELETE WHERE column = 'value'`), but the actual implementation of applying these conditions (`_delete_records` method) is currently a placeholder.
*   **Short Intervals:** The 1-second partition interval and 15-second retention period are intentionally short for quick demonstration. Real-world scenarios would use longer intervals (e.g., daily, weekly, monthly).

This project serves as a basic example of how `pg_partman` and custom scripting can be combined for data retention in PostgreSQL.
