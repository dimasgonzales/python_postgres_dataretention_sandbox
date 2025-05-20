import dataclasses
from typing import List, Optional
import datetime
import re
import psycopg # Added for type hinting db_connection

@dataclasses.dataclass
class DataRetentionPolicyConditions:
    column: str
    operator: str
    value: str

@dataclasses.dataclass
class DataRetentionPolicyTimeRetention:
    dt_target_column: str
    retention_seconds: int

@dataclasses.dataclass
class DataRetentionPolicy:
    timeretention: Optional[DataRetentionPolicyTimeRetention] = None
    conditions: Optional[List[DataRetentionPolicyConditions]] = None

    def validate(self):
        # XOR check if either timeretention or conditions is provided
        if not self.timeretention and not self.conditions:
            raise ValueError("Either timeretention or conditions must be provided")
        if self.timeretention and self.conditions:
            raise ValueError("Only one of timeretention or conditions can be provided")
        
        return True

@dataclasses.dataclass
class PostgresTableModel:
    table_name: str
    schema: str
    retention_policy: Optional[DataRetentionPolicy] # Made retention_policy optional to align with potential API design

    # Removed __init__ as dataclass generates it; will ensure retention_policy is passed if needed.
    # If retention_policy is truly optional for the model itself (not just the API), this is fine.
    # If it's always required for the model, the __init__ or a post_init might be needed
    # or ensure it's always provided during instantiation. For now, assuming it can be None
    # if no pruning is intended for a table instance.

    def validate(self, db_connection: psycopg.Connection) -> bool:
        table_exists = False
        with db_connection.cursor() as cursor:
            # Check if the table exists
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = %s AND table_schema = %s
                )
                """,
                (self.table_name, self.schema),
            )
            exists_result = cursor.fetchone()
            if exists_result:
                table_exists = exists_result[0]

        valid_policy = True # Default to true if no policy
        if self.retention_policy:
            valid_policy = self.retention_policy.validate()
        
        return table_exists and valid_policy

    def prune(self, db_connection: psycopg.Connection):
        if self.retention_policy:
            if self.retention_policy.timeretention:
                # Perform time-based retention
                self._drop_table_partitions(db_connection) # Renamed for clarity
            elif self.retention_policy.conditions:
                # Perform condition-based retention
                self._delete_records_by_condition(db_connection) # Renamed for clarity

    def _drop_table_partitions(self, db_connection: psycopg.Connection):
        # find all the child partitions of the table
        # then filter the partitions based on the retention policy
        # and drop them

        select_partitions = """
        SELECT
            child.relname AS partition_name
        FROM
            pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE
            parent.relname = %s AND parent.relnamespace = (
                SELECT oid FROM pg_namespace WHERE nspname = %s
            );
        """ # Added schema to parent table lookup for safety

        with db_connection.cursor() as cursor:
            cursor.execute(select_partitions, (self.table_name, self.schema))
            child_partitions = cursor.fetchall()
        
        if not self.retention_policy or not self.retention_policy.timeretention:
            # Should not happen if prune called this method, but good for safety
            print(f"Skipping partition drop for {self.schema}.{self.table_name}: no time retention policy.")
            return

        retention_seconds = self.retention_policy.timeretention.retention_seconds
        
        for partition_tuple in child_partitions:
            partition_name = partition_tuple[0]

            # Create a regex pattern to match the partition naming convention
            # Example: table_pYYYYMMDD_HHMMSS
            pattern = f"^{self.table_name}_p\\d{{8}}_\\d{{6}}$"
            if not re.match(pattern, partition_name):
                print(f"Skipping partition: {self.schema}.{partition_name} (doesn't match expected format {pattern})")
                continue

            try:
                # Extract the date part from the partition name
                # e.g., test_table1_p20230101_120000
                date_str_part = partition_name.split("_p")[1].split("_")[0] # YYYYMMDD
                time_str_part = partition_name.split("_")[-1] # HHMMSS
                
                partition_datetime_str = f"{date_str_part}{time_str_part}"
                partition_datetime = datetime.datetime.strptime(partition_datetime_str, "%Y%m%d%H%M%S").replace(tzinfo=datetime.timezone.utc)
            except (IndexError, ValueError) as e:
                print(f"Skipping partition: {self.schema}.{partition_name} (could not parse date/time: {e})")
                continue

            retention_delta = datetime.timedelta(seconds=retention_seconds)
            # Calculate the retention date: records older than this date (partitions representing time older than this) should be dropped
            # If a partition's timestamp is older than (now - retention_delta), it's eligible for pruning.
            cutoff_datetime = datetime.datetime.now(datetime.timezone.utc) - retention_delta
            
            if partition_datetime < cutoff_datetime:
                drop_partition_query = f"DROP TABLE IF EXISTS {self.schema}.{partition_name};" # Added semicolon
                print(f"Attempting to drop partition: {self.schema}.{partition_name} (Partition time: {partition_datetime}, Cutoff time: {cutoff_datetime})")
                try:
                    with db_connection.cursor() as cursor:
                        cursor.execute(drop_partition_query)
                    print(f"Dropped partition: {self.schema}.{partition_name}")
                except Exception as e_drop:
                    print(f"Error dropping partition {self.schema}.{partition_name}: {e_drop}")
                    # Potentially re-raise or log to a more persistent store
            else:
                print(f"Partition not dropped (still within retention period): {self.schema}.{partition_name} (Partition time: {partition_datetime}, Cutoff time: {cutoff_datetime})")
        
        db_connection.commit()
    
    def _delete_records_by_condition(self, db_connection: psycopg.Connection):
        # This method needs a more robust implementation for building SQL conditions safely.
        # For now, it's a placeholder as the original script didn't fully implement it.
        if not self.retention_policy or not self.retention_policy.conditions:
            print(f"Skipping record deletion for {self.schema}.{self.table_name}: no conditions policy.")
            return

        # WARNING: Building SQL queries by string formatting with user input is a security risk (SQL injection).
        # This part needs to be implemented carefully, ideally using query parameters.
        # Example: conditions = "column_name > %s" and then pass value as a parameter.
        # For multiple conditions, they need to be ANDed or ORed as appropriate.
        
        # Placeholder for condition building logic
        # conditions_clauses = []
        # params = []
        # for cond in self.retention_policy.conditions:
        #     # This is highly simplified and needs proper SQL construction and sanitization
        #     conditions_clauses.append(f"{cond.column} {cond.operator} %s")
        #     params.append(cond.value) # Need to ensure type compatibility with DB column
        # where_clause = " AND ".join(conditions_clauses)

        # if not where_clause:
        #     print(f"No valid conditions to apply for {self.schema}.{self.table_name}")
        #     return

        # query = f"DELETE FROM {self.schema}.{self.table_name} WHERE {where_clause};"
        
        # print(f"Attempting to delete records from {self.schema}.{self.table_name} with query: {query} and params: {params}")
        # try:
        #     with db_connection.cursor() as cursor:
        #         cursor.execute(query, tuple(params)) # Pass params as a tuple
        #         deleted_count = cursor.rowcount
        #         print(f"Deleted {deleted_count} records from {self.schema}.{self.table_name}")
        #     db_connection.commit()
        # except Exception as e_delete:
        #     print(f"Error deleting records from {self.schema}.{self.table_name}: {e_delete}")
        #     db_connection.rollback() # Rollback on error

        print(f"Condition-based deletion for {self.schema}.{self.table_name} is not fully implemented yet.")
        # For now, let's assume the original script's intent was to delete based on a single, simple condition string.
        # This is NOT SAFE for production.
        # Example: if self.retention_policy.conditions[0].column == 'status' and self.retention_policy.conditions[0].value == 'old'
        # query = f"DELETE FROM {self.schema}.{self.table_name} WHERE {self.retention_policy.conditions[0].column} = '{self.retention_policy.conditions[0].value}'"
        # with db_connection.cursor() as cursor:
        #     cursor.execute(query)
        #     db_connection.commit()
