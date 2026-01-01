import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path
import requests
from airflow.decorators import dag, task
 
CSV_PATH = Path("/home/marina/airflow/transactions.csv")
DATA_DIR = Path("/home/marina/airflow/data")
DB_PATH = DATA_DIR / "köksglädje.db"
SOURCE_URL = "http://schizoakustik.se/köksglädje/transactions.csv"
 
TRANSACTIONS_NORM = DATA_DIR / "transactions_normalized.csv"
DETAILS_NORM = DATA_DIR / "transaction_details_normalized.csv"
NEW_TRANSACTIONS = DATA_DIR / "new_transactions.csv"
NEW_DETAILS = DATA_DIR / "new_transaction_details.csv"
 
 
@dag(
    dag_id="koks_etl_deco",
    start_date=datetime(2025, 12, 23),
    schedule="@daily",
    catchup=False,
    tags=["tuc"]
)
def koks_etl_decorators():
 
    @task
    def download_csv() -> None:
        CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
 
        response = requests.get(SOURCE_URL, timeout=30)
        response.raise_for_status()
        CSV_PATH.write_text(response.text, encoding="utf-8")
 
    @task
    def normalize_data() -> None:
        df = pd.read_csv(CSV_PATH, dtype=str)
        # The format ISO8601 handles both of the date formats present in the data
        df["TransactionDate"] = pd.to_datetime(df["TransactionDate"].astype(str).str.strip(), errors="coerce", format="ISO8601")
 
        if not (df["TransactionID"] == df["TransactionID.1"]).all():
            raise ValueError("TransactionID mismatch mellan TransactionID och TransactionID.1")
 
        transaction_columns = [
            "TransactionID",
            "StoreID",
            "CustomerID",
            "TransactionDate",
            "TotalAmount",
        ]
 
        transaction_details_columns = [
            "TransactionDetailID",
            "TransactionID.1",
            "ProductID",
            "CampaignID",
            "Quantity",
            "TotalPrice",
            "PriceAtPurchase",
        ]
 
        transactions_df = df[transaction_columns].drop_duplicates()
        transaction_details_df = df[transaction_details_columns]
        transactions_df["TransactionDate"] = transactions_df["TransactionDate"].dt.strftime("%Y-%m-%d %H:%M:%S")
 
        transactions_df.to_csv(TRANSACTIONS_NORM, index=False)
        transaction_details_df.to_csv(DETAILS_NORM, index=False)
 
    @task
    def create_database() -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
 
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
 
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
 
    @task
    def filter_new_transactions() -> None:
        
        transactions_df = pd.read_csv(TRANSACTIONS_NORM)
        details_df = pd.read_csv(DETAILS_NORM)

        # Ensure consistent types
        transactions_df["TransactionID"] = transactions_df["TransactionID"].astype("int64")
        details_df["TransactionID.1"] = details_df["TransactionID.1"].astype("int64")

        con = sqlite3.connect(DB_PATH)

        # If table exists, read existing IDs; otherwise treat as empty DB
        try:
            existing_ids = pd.read_sql("SELECT TransactionID FROM Transactions;", con)["TransactionID"]
            existing_ids = set(existing_ids.astype("int64").tolist())
        except Exception:
            existing_ids = set()

        con.close()

        new_transactions = transactions_df[~transactions_df["TransactionID"].isin(existing_ids)]
        new_details = details_df[details_df["TransactionID.1"].isin(new_transactions["TransactionID"])]

        new_transactions.to_csv(NEW_TRANSACTIONS, index=False)
        new_details.to_csv(NEW_DETAILS, index=False)


    @task
    def load_to_database() -> None:
        
        new_transactions = pd.read_csv(NEW_TRANSACTIONS)
        new_details = pd.read_csv(NEW_DETAILS)

        if len(new_transactions) == 0:
            return

        con = sqlite3.connect(DB_PATH)

        new_transactions.to_sql("Transactions", con=con, if_exists="append", index=False)

        new_details = new_details.rename(columns={"TransactionID.1": "TransactionID"})
        new_details.to_sql("TransactionDetails", con=con, if_exists="append", index=False)

        con.commit()
        con.close()
 
    t1 = download_csv()
    t2 = normalize_data()
    t3 = create_database()
    t4 = filter_new_transactions()
    t5 = load_to_database()
 
    t1 >> t2 >> t3 >> t4 >> t5
 
koks_etl_decorators()
