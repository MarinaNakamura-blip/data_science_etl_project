# Coded on Jupyter notebook

import pandas as pd
import seaborn as sns
import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt


DB_PATH = Path("/home/marina/airflow/data/köksglädje.db") 
con = sqlite3.connect(DB_PATH)

#-----------------------------------------------------------------------------------------------------------------------------
transactions = pd.read_sql("SELECT * FROM Transactions;", con)
details = pd.read_sql("SELECT * FROM TransactionDetails;", con)
transactions["TransactionDate"] = pd.to_datetime(transactions["TransactionDate"], errors="coerce", format="mixed")
#-----------------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------------
transactions_per_day = (transactions.dropna(subset=["TransactionDate"]).groupby(transactions["TransactionDate"].dt.date).size())
transactions_per_day.index = pd.to_datetime(transactions_per_day.index)
plt.scatter(transactions_per_day.index, transactions_per_day.values, s=10)

plt.xlabel("Year")
plt.ylabel("Number of transactions")
plt.title("Number of Transactions per Day")
plt.show()
#-----------------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------------
# Total sales per store with store names, using a JOIN (merge)
stores = pd.read_sql("SELECT StoreID, StoreName FROM Stores;", con)

sales_by_store = (transactions.groupby("StoreID")["TotalAmount"].sum().reset_index())
sales_by_store = sales_by_store.merge(stores, on="StoreID", how="left")
sales_by_store = sales_by_store.sort_values("TotalAmount", ascending=False)

plt.figure(figsize=(8, 5))
sns.barplot(data=sales_by_store, x="StoreName", y="TotalAmount")

plt.title("Total sales per store")
plt.xlabel("Store Name")
plt.ylabel("Total sales")
plt.xticks(rotation=45, ha="right")
plt.show()

# This is almost equivalent to this SQL query:
# SELECT
#     t.StoreID,
#     SUM(t.TotalAmount) AS TotalAmount,
#     s.StoreName
# FROM Transactions t
# LEFT JOIN Stores s
#     ON t.StoreID = s.StoreID
# GROUP BY t.StoreID, s.StoreName;
#-----------------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------------
# Same as above but letting seaborn do the aggregation
stores = pd.read_sql("SELECT StoreID, StoreName FROM Stores;", con)
transactions_with_store = transactions.merge(stores, on="StoreID", how="left")

ax = sns.barplot(data=transactions_with_store, x="StoreName", y="TotalAmount", estimator="sum", errorbar=None)
 
ax.set_title("Total sales per store")
ax.set_xlabel("Store Name")
ax.set_ylabel("Total sales")

plt.xticks(rotation=45, ha="right")
plt.show()
#-----------------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------------
sales_by_store = (transactions_with_store.groupby("StoreName")["TotalAmount"].sum())

plt.figure(figsize=(8, 8))
plt.pie(sales_by_store, labels=sales_by_store.index, autopct="%1.1f%%", startangle=90)
plt.title("Share of Total Sales per Store")
plt.show()
#-----------------------------------------------------------------------------------------------------------------------------
