import snowflake.connector
import os


def get_db_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USERNAME"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT_ID"],
        warehouse=os.environ["WAREHOUSE"],
        database=os.environ["DATABASE"],
        schema=os.environ["SCHEMA"],
    )
