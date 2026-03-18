import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os

# Load credentials from .env file
load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="ECOMMERCE_WH",
    database="ECOMMERCE_DB",
    schema="RAW",
    role="ACCOUNTADMIN"
)

files = {
    "RAW_ORDERS":               "olist_orders_dataset.csv",
    "RAW_ORDER_ITEMS":          "olist_order_items_dataset.csv",
    "RAW_ORDER_PAYMENTS":       "olist_order_payments_dataset.csv",
    "RAW_ORDER_REVIEWS":        "olist_order_reviews_dataset.csv",
    "RAW_CUSTOMERS":            "olist_customers_dataset.csv",
    "RAW_SELLERS":              "olist_sellers_dataset.csv",
    "RAW_PRODUCTS":             "olist_products_dataset.csv",
    "RAW_CATEGORY_TRANSLATION": "product_category_name_translation.csv",
    "RAW_GEOLOCATION":          "olist_geolocation_dataset.csv",
}

data_path = Path("data/raw")

for table_name, filename in files.items():
    filepath = data_path / filename
    print(f"Loading {filename}...")
    df = pd.read_csv(filepath)
    df.columns = [col.upper() for col in df.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name,
        auto_create_table=True,
        overwrite=True
    )
    print(f"  ✅ {table_name}: {nrows:,} rows loaded")

conn.close()
print("\n🎉 All tables loaded successfully!")