#!/bin/bash

docker compose down
docker compose up -d

# Set PostgreSQL environment variables
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGDATABASE=postgres
export PGPASSWORD=postgres

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
while ! pg_isready -h localhost -p 5432 -U postgres > /dev/null 2>&1; do
    echo "Postgres is still starting up. Waiting..."
    sleep 2
done
echo "PostgreSQL is ready!"


# Execute the initialize_postgres.sql script with psql
echo "Executing database initialization script..."
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f scripts/initialize_postgres.sql
echo "Database initialization completed."


# Start data insertion loop for 60 seconds
echo "Starting data insertion loop for 60 seconds..."
end_time=$(($(date +%s) + 60))

while [ $(date +%s) -lt $end_time ]; do
    remaining=$((end_time - $(date +%s)))
    echo "Executing data insertion script... ($remaining seconds remaining)"
    psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f scripts/insert_data_into_testtable1.sql
    echo "Data insertion completed."

    echo "Running maintenance on test_table1..."
    psql -c "SELECT public.run_maintenance('public.test_table1')"
    echo "Maintenance completed."

    # Don't sleep if we're about to exit the loop
    if [ $(date +%s) -lt $((end_time - 2)) ]; then
        sleep 2
    fi
done
echo "60-second insertion loop completed."

./prune_postgres.py