from utils import get_db_connection
from pathlib import Path

customer_info = "cust_info"
product_info = "prd_info"
sales_details = "sales_details"
cust_az12 = "cust_az12"
loc_a101 = "loc_a101"
px_cat_g1v2 = "px_cat_g1v2"

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


def create_landing_tables():
    with get_db_connection() as conn:
        for k, v in creation_statements.items():
            conn.cursor().execute(v)


def load_data():
    PROJ_ROOT = Path(__file__).resolve().parent
    DATA_DIR = PROJ_ROOT / "data" / "raw"
    source_systems = ["crm", "erp"]
    for _source in source_systems:
        # get file_paths
        path = DATA_DIR / f"source_{_source}"
        csv_files = [file for file in path.iterdir() if file.suffix == ".csv"]
        for file in csv_files:
            stem = file.stem.lower()
            if stem in creation_statements.keys():
                with get_db_connection() as conn:
                    print(f"Running for file: {stem}")
                    print(f"file path: {file}")
                    put_sql = f"PUT file://{file} @%{_source}_{stem}"
                    print(put_sql)
                    conn.cursor().execute(put_sql)
                    copy_sql = f"""COPY INTO {_source}_{stem} FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)"""
                    print(copy_sql)
                    conn.cursor().execute(copy_sql)
            else:
                continue


if __name__ == "__main__":
    create_landing_tables()
    load_data()
