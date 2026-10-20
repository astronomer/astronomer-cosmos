import os
import re

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector.errors import ProgrammingError


def load_private_key() -> bytes:
    private_key = serialization.load_pem_private_key(os.environ["SNOWFLAKE_PRIVATE_KEY"].encode(), password=None)
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def delete_snowflake_resource():
    """
    Delete Snowflake resources with a given prefix(set as an environment variable).
    """
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        private_key=load_private_key(),
        role=os.environ["SNOWFLAKE_ROLE"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    prefix = os.getenv("RESOURCE_PREFIX", "")
    if prefix:
        cursor = conn.cursor()
        # Use parameterized query for the SELECT (safe for values)
        query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_name LIKE %s
        """
        cursor.execute(query, (os.environ["SNOWFLAKE_SCHEMA"], f"{prefix}_%"))
        resources = cursor.fetchall()
        if re.match(r"^[A-Za-z0-9_]+$", prefix):
            # Semantic views are not listed in information_schema.tables
            cursor.execute(f"SHOW SEMANTIC VIEWS LIKE '{prefix}_%'")
            resources += [
                (row[1], "SEMANTIC VIEW")
                for row in cursor.fetchall()
                if row[1].startswith(f"{prefix.upper()}_")
            ]

        for resource_name, resource_type in resources:
            # Validate table name contains only safe characters (prevent injection)
            if not re.match(r"^[A-Z0-9_]+$", resource_name):
                print(f"Skipping potentially unsafe table name: {resource_name}")
                continue

            try:
                if resource_type == "BASE TABLE":
                    # Table names must be part of SQL string, not parameterized
                    drop_sql = f"DROP TABLE IF EXISTS {resource_name}"
                    print(f"Executing: {drop_sql}")
                    cursor.execute(drop_sql)
                elif resource_type == "VIEW":
                    drop_sql = f"DROP VIEW IF EXISTS {resource_name}"
                    print(f"Executing: {drop_sql}")
                    cursor.execute(drop_sql)
                elif resource_type == "SEMANTIC VIEW":
                    drop_sql = f"DROP SEMANTIC VIEW IF EXISTS {resource_name}"
                    print(f"Executing: {drop_sql}")
                    cursor.execute(drop_sql)
            except ProgrammingError as e:
                print(f"Failed to drop {resource_name}: {e}")

        cursor.close()
        print(f"Processed {len(resources)} resources")
    else:
        print("No RESOURCE_PREFIX set, skipping cleanup")
    conn.close()


if __name__ == "__main__":
    delete_snowflake_resource()
