# ----------------------------
# Imports
# ----------------------------
# pandas: used for reading CSV files and transforming tabular data
import pandas as pd

# sqlite3: used to create and interact with a local SQLite database
import sqlite3

# datetime: required by Airflow to define the DAG start date
from datetime import datetime

# Path: provides clean and safe handling of file system paths
from pathlib import Path

# requests: used to download the CSV file from a remote URL
import requests

# Airflow decorators: simplify DAG and task definitions
from airflow.decorators import dag, task


# ----------------------------
# File paths and constants
# ----------------------------
# Local path where the raw CSV file will be downloaded
CSV_PATH = Path("/home/marina/airflow/transactions.csv")

# Directory where processed data and the database are stored
DATA_DIR = Path("/home/marina/airflow/data")

# SQLite database file path
DB_PATH = DATA_DIR / "köksglädje.db"

# Source URL for the raw transaction data
SOURCE_URL = "http://schizoakustik.se/köksglädje/transactions.csv"

# Normalized output files (used for transparency and debugging)
TRANSACTIONS_NORM = DATA_DIR / "transactions_normalized.csv"
DETAILS_NORM = DATA_DIR / "transaction_details_normalized.csv"

# Files that contain only new records not yet loaded into the database
NEW_TRANSACTIONS = DATA_DIR / "new_transactions.csv"
NEW_DETAILS = DATA_DIR / "new_transaction_details.csv"


# ----------------------------
# DAG definition
# ----------------------------
# @dag defines the Airflow workflow
# dag_id: identifier shown in the Airflow UI
# start_date: when Airflow starts scheduling the DAG
# schedule: runs once per day
# catchup=False: avoids backfilling historical runs
# tags: UI categorization
@dag(
    dag_id="koks_etl_deco",
    start_date=datetime(2025, 12, 23),
    schedule="@daily",
    catchup=False,
    tags=["tuc"]
)
def koks_etl_decorators():

    # ----------------------------
    # TASK 1: Download raw CSV
    # ----------------------------
    # This task:
    # - ensures required directories exist
    # - downloads the CSV from the source URL
    # - saves it locally
    @task
    def download_csv() -> None:
        CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        response = requests.get(SOURCE_URL, timeout=30)

        # Raise an exception if the request fails (Airflow marks task as failed)
        response.raise_for_status()

        # Write the downloaded content to a local CSV file
        CSV_PATH.write_text(response.text, encoding="utf-8")


    # ----------------------------
    # TASK 2: Normalize data
    # ----------------------------
    # This task:
    # - reads the raw CSV
    # - cleans and parses dates
    # - splits the dataset into two normalized tables
    # - saves the results as CSV files
    @task
    def normalize_data() -> None:
        # Read all columns as strings to avoid incorrect type inference
        df = pd.read_csv(CSV_PATH, dtype=str)

        # Parse TransactionDate using ISO8601 (handles multiple date formats)
        df["TransactionDate"] = pd.to_datetime(
            df["TransactionDate"].astype(str).str.strip(),
            errors="coerce",
            format="ISO8601"
        )

        # Validate that both transaction ID columns match
        if not (df["TransactionID"] == df["TransactionID.1"]).all():
            raise ValueError("TransactionID mismatch mellan TransactionID och TransactionID.1")

        # Columns representing the main transaction entity
        transaction_columns = [
            "TransactionID",
            "StoreID",
            "CustomerID",
            "TransactionDate",
            "TotalAmount",
        ]

        # Columns representing transaction line details
        transaction_details_columns = [
            "TransactionDetailID",
            "TransactionID.1",
            "ProductID",
            "CampaignID",
            "Quantity",
            "TotalPrice",
            "PriceAtPurchase",
        ]

        # Create normalized tables
        transactions_df = df[transaction_columns].drop_duplicates()
        transaction_details_df = df[transaction_details_columns]

        # Convert datetime to a consistent string format
        transactions_df["TransactionDate"] = transactions_df["TransactionDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Save normalized outputs
        transactions_df.to_csv(TRANSACTIONS_NORM, index=False)
        transaction_details_df.to_csv(DETAILS_NORM, index=False)


    # ----------------------------
    # TASK 3: Create database and tables
    # ----------------------------
    # This task:
    # - creates the SQLite database if missing
    # - creates required tables if they do not already exist
    @task
    def create_database() -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()

        # Create Transactions table (one row per transaction)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Transactions (
                TransactionID INTEGER PRIMARY KEY,
                StoreID INTEGER,
                CustomerID INTEGER,
                TransactionDate TEXT,
                TotalAmount REAL
            );
            """
        )

        # Create TransactionDetails table (one-to-many relationship)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS TransactionDetails (
                TransactionDetailID INTEGER PRIMARY KEY,
                TransactionID INTEGER,
                ProductID INTEGER,
                CampaignID INTEGER,
                Quantity INTEGER,
                TotalPrice REAL,
                PriceAtPurchase REAL,
                FOREIGN KEY(TransactionID) REFERENCES Transactions(TransactionID)
            );
            """
        )

        con.commit()
        con.close()


    # ----------------------------
    # TASK 4: Filter new transactions
    # ----------------------------
    # This task ensures incremental loading:
    # - compares incoming TransactionIDs with those already in the database
    # - keeps only new transactions and their related details
    @task
    def filter_new_transactions() -> None:
        transactions_df = pd.read_csv(TRANSACTIONS_NORM)
        details_df = pd.read_csv(DETAILS_NORM)

        # Ensure consistent integer types for comparisons
        transactions_df["TransactionID"] = transactions_df["TransactionID"].astype("int64")
        details_df["TransactionID.1"] = details_df["TransactionID.1"].astype("int64")

        con = sqlite3.connect(DB_PATH)

        # Read existing TransactionIDs from database if table exists
        try:
            existing_ids = pd.read_sql(
                "SELECT TransactionID FROM Transactions;",
                con
            )["TransactionID"]
            existing_ids = set(existing_ids.astype("int64").tolist())
        except Exception:
            # If table is missing or empty, treat DB as empty
            existing_ids = set()

        con.close()

        # Filter out transactions already present in the database
        new_transactions = transactions_df[~transactions_df["TransactionID"].isin(existing_ids)]

        # Keep only detail rows linked to new transactions
        new_details = details_df[
            details_df["TransactionID.1"].isin(new_transactions["TransactionID"])
        ]

        new_transactions.to_csv(NEW_TRANSACTIONS, index=False)
        new_details.to_csv(NEW_DETAILS, index=False)


    # ----------------------------
    # TASK 5: Load data into database
    # ----------------------------
    # This task:
    # - loads only new transactions into SQLite
    # - skips execution if no new data exists
    @task
    def load_to_database() -> None:
        new_transactions = pd.read_csv(NEW_TRANSACTIONS)
        new_details = pd.read_csv(NEW_DETAILS)

        # Exit early if there is nothing new to insert
        if len(new_transactions) == 0:
            return

        con = sqlite3.connect(DB_PATH)

        # Append new transactions
        new_transactions.to_sql(
            "Transactions",
            con=con,
            if_exists="append",
            index=False
        )

        # Rename column to match database schema
        new_details = new_details.rename(columns={"TransactionID.1": "TransactionID"})

        # Append new transaction details
        new_details.to_sql(
            "TransactionDetails",
            con=con,
            if_exists="append",
            index=False
        )

        con.commit()
        con.close()


    # ----------------------------
    # Task dependencies
    # ----------------------------
    # Instantiate tasks
    t1 = download_csv()
    t2 = normalize_data()
    t3 = create_database()
    t4 = filter_new_transactions()
    t5 = load_to_database()

    # Define execution order
    t1 >> t2 >> t3 >> t4 >> t5


# ----------------------------
# Register DAG with Airflow
# ----------------------------
koks_etl_decorators()
