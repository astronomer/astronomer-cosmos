import os

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
        cursor.execute(
            f"""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = '{os.environ['SNOWFLAKE_SCHEMA']}'
            AND table_name LIKE '{prefix}_%'
            """
        )

        resources = cursor.fetchall()

        for resource_name, resource_type in resources:
            if resource_type == "BASE TABLE":
                cursor.execute(f"DROP TABLE IF EXISTS {resource_name}")
            elif resource_type == "VIEW":
                cursor.execute(f"DROP VIEW IF EXISTS {resource_name}")
        cursor.close()
        print(f"Deleted {len(resources)} resources")
    else:
        print("No resources to delete")
    conn.close()


if __name__ == "__main__":
    delete_snowflake_resource()
