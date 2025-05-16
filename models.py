import dataclasses

from typing import List
import datetime
import re

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
    timeretention: DataRetentionPolicyTimeRetention
    conditions: List[DataRetentionPolicyConditions]

    def validate(self):
        #xor check if either timeretention or conditions is provided

        if not self.timeretention and not self.conditions:
            raise ValueError("Either timeretention or conditions must be provided")
        if self.timeretention and self.conditions:
            raise ValueError("Only one of timeretention or conditions can be provided")
        
        return True

@dataclasses.dataclass
class PostgresTableModel:
    table_name: str
    schema: str

    def __init__(self, table_name: str, schema: str, retention_policy: DataRetentionPolicy):
        self.table_name = table_name
        self.schema = schema
        self.retention_policy = retention_policy

    def validate(self, db_connection):

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
            exists = cursor.fetchone()[0]

            if not exists:
                table_exists = False
            table_exists = True

        valid_table = valid_table = self.retention_policy.validate() if self.retention_policy else True
        
        return table_exists & valid_table


    def prune(self, db_connection):
        if self.retention_policy:
            if self.retention_policy.timeretention:
                # Perform time-based retention
                self._drop_table(db_connection)
            elif self.retention_policy.conditions:
                # Perform condition-based retention
                self._delete_records(db_connection)

    def _drop_table(self, db_connection):

        # find all the child partitions of the table
        # then filter the partitions based on the retention policy
        # and drop them

        select_partitions = f"""
        SELECT
            child.relname AS partition_name
        FROM
            pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE
            parent.relname = '{self.table_name}';
        """

        with db_connection.cursor() as cursor:
            cursor.execute(select_partitions)
            child_partitions = cursor.fetchall()
        
        for partition in child_partitions:
            partition_name = partition[0]

            # Create a regex pattern to match the partition naming convention
            pattern = f"^{self.table_name}_p\\d{{8}}_\\d{{6}}$"
            if not re.match(pattern, partition_name):
                # Skip this partition as it doesn't match the expected format
                print(f"Skipping partition: {partition_name} (doesn't match expected format)")
                continue




            # Assuming the partition name is in the format test_table1_pYYYYMMDD_HHMMSS
            # Extract the date part from the partition name
            partition_date_str = partition_name.split("_")[-2]
            partition_date = partition_date_str[1:9]
            partition_time_str = partition_name.split("_")[-1]
            partition_datetime = datetime.datetime.strptime(f"{partition_date} {partition_time_str}", "%Y%m%d %H%M%S").replace(tzinfo=datetime.timezone.utc)
            # Convert the partition datetime string to a datetime object
            # Assuming the retention policy is in seconds
            retention_seconds = self.retention_policy.timeretention.retention_seconds
            retention_time = datetime.timedelta(seconds=retention_seconds)
            # Calculate the retention date
            retention_date = datetime.datetime.now() - retention_time
            retention_date = retention_date.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=-5)))
            # Check if the partition date is older than the retention date
            if partition_datetime < retention_date:
                # Drop the partition
                drop_partition_query = f"DROP TABLE IF EXISTS {self.schema}.{partition_name}"
                with db_connection.cursor() as cursor:
                    cursor.execute(drop_partition_query)
                    print(f"Dropped partition: {partition_name}")
            else:
                print(f"Partition not dropped: {partition_name}")

        
        db_connection.commit()


        with db_connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.schema}.{self.table_name}")
            db_connection.commit()
    
    def _delete_records(self, db_connection):
        with db_connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {self.schema}.{self.table_name} WHERE condition")
            db_connection.commit()


