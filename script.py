import os
import time   # to add sleep on requests.get() function, Polygon.io Free
import csv    # to create a csv with the json data
import requests
import traceback
from datetime import datetime #added to track task scheduler run
from dotenv import load_dotenv
import snowflake.connector # to export tickers.csv to snowflake
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000
DS = datetime.now().strftime('%Y-%m-%d')

# print(POLYGON_API_KEY)

# First API Request
url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
tickers = []

data = response.json()
for ticker in data['results']:
    ticker['ds'] = DS # to track last update from polygon.io in snowflake
    tickers.append(ticker)
    
# handle API data pagination
while 'next_url' in data:
    print('requesting next page', data['next_url'])
    time.sleep(20)  # wait 20 seconds between calls
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()
    print(data)

    for ticker in data['results']:
        ticker['ds'] = DS # to track last update from script.py in snowflake YYYY-MM-DD
        tickers.append(ticker)

# print(len(tickers))

# Schema from example_ticker to create csv
example_ticker = {
        "ticker": "ZTRE",
        "name": "F/m 3-Year Investment Grade Corporate Bond ETF",
        "market": "stocks",
        "locale": "us",
        "primary_exchange": "XNAS",
        "type": "ETF",
        "active": True,
        "currency_name": "usd",
        "composite_figi": "BBG01KRNY4P1",
        "share_class_figi": "BBG01KRNY5K3",
        "last_updated_utc": "2025-09-25T06:05:34.54325405Z",
        "ds": "2025-09-25" }
fieldnames = list(example_ticker.keys())        

# Run code to update tickers.csv
def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    # Write tickers to CSV with example_ticker schema
    fieldnames = list(example_ticker.keys())
    output_file = "tickers.csv"
    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in tickers:
            # ensure missing keys don’t break the write
            filtered_row = {field: row.get(field, '') for field in fieldnames}
            writer.writerow(filtered_row)

    print(f"✅ Data written {len(tickers)} to {output_file}")
    log_run(f"✅ Data written {len(tickers)} rows to {output_file}")

def load_to_snowflake(rows, fieldnames):
    """
    Load ticker data into Snowflake.
    Creates the stock_tickers table if it does not exist.
    """

    # 1. Build connection kwargs from env vars
    connect_kwargs = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),   # e.g., xy12345.us-east-1
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "client_session_keep_alive": True,
        "client_telemetry_enabled": False,
    }

    print("🔑 Snowflake connection details (safe):")
    print({k: v for k, v in connect_kwargs.items() if k != "password"})

    # 2. Convert rows (list of dicts) into DataFrame
    df = pd.DataFrame(rows, columns=fieldnames)

    # 3. Ensure Snowflake-compatible column names
    df.columns = (
        df.columns.str.strip()         # remove leading/trailing spaces
            .str.upper()         # make them uppercase (Snowflake convention)
            .str.replace(" ", "_")  # replace spaces with underscores
    )

    print(df.columns)  # sanity check

    # 4. Explicit SQL schema definition
    types_overrides = """
        ticker VARCHAR,
        name VARCHAR,
        market VARCHAR,
        locale VARCHAR,
        primary_exchange VARCHAR,
        type VARCHAR,
        active BOOLEAN,
        currency_name VARCHAR,
        composite_figi VARCHAR,
        share_class_figi VARCHAR,
        last_updated_utc TIMESTAMP_TZ,
        ds VARCHAR
    """

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS stock_tickers (
        {types_overrides}
    )
    """

    try:
        # 5. Connect to Snowflake
        conn = snowflake.connector.connect(**connect_kwargs)
        cursor = conn.cursor()

        # 6. Ensure table exists with fixed schema
        cursor.execute(create_table_sql)
        print("✅ Ensured stock_tickers table exists with explicit schema")

        # 7. Upload with write_pandas (overwrite mode by default)
        success, nchunks, nrows, _ = write_pandas(
            conn, df, table_name="STOCK_TICKERS", overwrite=True
        )

        print(f"✅ Uploaded {nrows} rows to Snowflake (success={success}, chunks={nchunks})")

    except Exception as e:
        print(f"❌ Error loading to Snowflake: {e}")
        traceback.print_exc()  # <-- this shows full error details and line numbers

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

# Log function to track task performance and traceability
def log_run(msg="Task ran"):
    """Append log messages with timestamp to task_log.txt"""
    with open("task_log.txt", "a", encoding="utf-8") as f:
        f.write(f"{msg} at {datetime.now()}\n")

# 
if __name__ == '__main__':
    log_run("🚀 Scheduled Task started")

    # Update tickers.csv
    run_stock_job()

     # Load tickers to Snowflake
    load_to_snowflake(tickers, fieldnames)

    # Print first few tickers that were injected
    print("📊 Sample tickers injected to Snowflake:")
    for t in tickers[:10]:   # show first 10
        print("-", t.get("ticker"))

    log_run("✅ Scheduled Task finished")

