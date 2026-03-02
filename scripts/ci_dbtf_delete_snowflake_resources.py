import os
import re

import snowflake.connector


def delete_snowflake_resource():
    """
    Delete Snowflake resources with a given prefix(set as an environment variable).
    """
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
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
            except Exception as e:
                print(f"Failed to drop {resource_name}: {e}")

        cursor.close()
        print(f"Processed {len(resources)} resources")
    else:
        print("No RESOURCE_PREFIX set, skipping cleanup")
    conn.close()


if __name__ == "__main__":
    delete_snowflake_resource()
