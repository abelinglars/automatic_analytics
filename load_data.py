import snowflake.connector
import os

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USERNAME"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT_ID"],
    warehouse=os.environ["WAREHOUSE"],
    database=os.environ["DATABASE"],
    schema=os.environ["SCHEMA"],
)

conn.cursor().execute(
    "PUT file:///Users/lars/work/automatic_analytics/data/raw/source_crm/* @%crm_staging"
)
