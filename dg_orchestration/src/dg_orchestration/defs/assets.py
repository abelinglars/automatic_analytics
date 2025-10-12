import os
import subprocess
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext, AssetOut, Output, ResourceDefinition

from dagster_dbt import (
    DbtCliResource,
    DbtCliInvocation,
    dbt_assets,
    DbtProject,
    get_asset_keys_by_output_name_for_source,
    get_asset_key_for_model
)

import snowflake.connector

from dg_orchestration.defs.resources import (
    dbt_project,
    dbt_resource,
    dbt_proj_directory
)

# for data loading
PROJ_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
DATA_DIR = PROJ_ROOT / "data" / "raw"


# utility to get db connection from env vars
def get_db_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USERNAME"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT_ID"],
        warehouse=os.environ["WAREHOUSE"],
        database=os.environ["DATABASE"],
        schema=os.environ["SCHEMA"],
    )


# for matching csv files to table names in bronze layer
customer_info = "cust_info"
product_info = "prd_info"
sales_details = "sales_details"
cust_az12 = "cust_az12"
loc_a101 = "loc_a101"
px_cat_g1v2 = "px_cat_g1v2"


# table creation statements for bronze layer
creation_statements = {
    customer_info: f"""
    CREATE OR REPLACE TABLE crm_{customer_info} (
            cst_id INT,
            cst_key TEXT,
            cst_firstname TEXT,
            cst_lastname TEXT,
            cst_marital_status TEXT,
            cst_gndr TEXT,
            cst_create_date TEXT
    );
    """,
    product_info: f"""
    CREATE OR REPLACE TABLE crm_{product_info} (
            prd_id INTEGER,
            prd_key TEXT,
            prd_nm TEXT,
            prd_cost INTEGER,
            prd_line TEXT,
            prd_start_dt DATE,
            prd_end_dt DATE
    );
    """,
    sales_details: f"""
    CREATE OR REPLACE TABLE crm_{sales_details} (
            sls_ord_num TEXT,
            sls_prd_key TEXT,
            sls_cust_id INTEGER,
            sls_order_dt INTEGER,
            sls_ship_dt INTEGER,
            sls_due_dt INTEGER,
            sls_sales INTEGER,
            sls_quantity INTEGER,
            sls_price INTEGER
    );
    """,
    cust_az12: f"""
    CREATE OR REPLACE TABLE erp_{cust_az12} (
            CID TEXT,
            BDATE DATE,
            GEN TEXT
    );
    """,
    loc_a101: f"""
    CREATE OR REPLACE TABLE erp_{loc_a101} (
            CID TEXT,
            CNTRY TEXT
    );
    """,
    px_cat_g1v2: f"""
    CREATE OR REPLACE TABLE erp_{px_cat_g1v2} (
            ID TEXT,
            CAT TEXT,
            SUBCAT TEXT,
            MAINTENANCE BOOLEAN
    );
    """,
}


# create tables in bronze
def create_landing_tables():
    with get_db_connection() as conn:
        for k, v in creation_statements.items():
            conn.cursor().execute(v)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project = dbt_project
)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from (
        dbt.cli(["build"], context=context).stream().fetch_column_metadata()
    )


@dg.multi_asset(
    outs={
        name: AssetOut(key=asset_key)
        for name, asset_key in get_asset_keys_by_output_name_for_source(
            [my_dbt_assets], "crm"
        ).items()
    },
)
def crm(context: AssetExecutionContext):
    output_names = list(context.op_execution_context.selected_output_names)
    yield Output(value=..., output_name=output_names[0])
    yield Output(value=..., output_name=output_names[1])
    yield Output(value=..., output_name=output_names[2])


@dg.asset(deps = [get_asset_key_for_model([my_dbt_assets], "fct_sales")])
def monthly_report():
    output_path = Path().home() / "work" / "automatic_analytics" / "dg_orchestration" / "reports" 
    quarto_path =  output_path / "monthly_sales_report.qmd"

    process = subprocess.Popen(
            ["quarto", "render", quarto_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    while True:
        line = process.stdout.readline()
        if not line:
            break
        dg.get_dagster_logger().info(line.strip())

def load_source(source_system):
    path = DATA_DIR / f"source_{source_system}"
    csv_files = [file for file in path.iterdir() if file.suffix == ".csv"]

    for file in csv_files:
        table_name = file.stem.lower()

        # only accept vetted filenames
        if table_name in creation_statements.keys():
            with get_db_connection() as conn:
                put_sql = f"PUT file://{file} @%{source_system}_{table_name}"
                copy_sql = f"""COPY INTO {source_system}_{table_name} FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)"""

                conn.cursor().execute(put_sql)
                conn.cursor().execute(copy_sql)
        else:
            continue


@dg.asset
def load_erp():
    load_source("erp")


@dg.asset
def load_crm():
    load_source("crm")


# daily refresh from source
daily_load = dg.ScheduleDefinition(
    name = "daily_load",
    cron_schedule = "0 0 * * *",
    target = [load_crm, load_erp]
)
