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

sales_by_store = (transactions.groupby("StoreID")["TotalAmount"].sum().sort_values(ascending=False).reset_index())
sales_by_store = sales_by_store.merge(stores, on="StoreID", how="left")

plt.figure(figsize=(7, 5))
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
transactions_with_store = transactions.merge(stores, on="StoreID", how="left")

ax = sns.barplot(data=transactions_with_store, x="StoreName", y="TotalAmount", estimator="sum", errorbar=None)
 
ax.set_title("Total sales per store")
ax.set_xlabel("Store Name")
ax.set_ylabel("Total sales")

plt.xticks(rotation=45, ha="right")
plt.show()
#-----------------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------------
sales_by_store = (transactions_with_store.groupby("StoreName")["TotalAmount"].sum().sort_values(ascending=False))

plt.figure(figsize=(6, 6))
plt.pie(sales_by_store, labels=sales_by_store.index, autopct="%1.1f%%", startangle=90)
plt.title("Share of Total Sales per Store")
plt.show()
#-----------------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------------
# Same as above but only for 2022, with a different color palette and a legend
transactions_2022 = transactions_with_store[transactions_with_store["TransactionDate"].dt.year == 2022]
sales_by_store_2022 = (transactions_2022.groupby("StoreName")["TotalAmount"].sum().sort_values(ascending=False))

plt.figure(figsize=(6, 6))
plt.pie(sales_by_store_2022, autopct="%1.1f%%", startangle=90, colors=plt.cm.tab20.colors)
plt.title("Share of Total Sales per Store (2022)")
plt.legend(sales_by_store_2022.index, title="Store Name", loc="center left", bbox_to_anchor=(1, 0.5))
plt.show()
#-----------------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------------
products = pd.read_sql("SELECT ProductID, ProductName FROM Products;", con) # Load the product table
transactions_2022 = transactions[transactions["TransactionDate"].dt.year == 2022] # Filter the transactions in 2022 (2022 was chosen because there are many data in that year)

details_2022 = details.merge(transactions_2022[["TransactionID"]], on="TransactionID", how="inner") # Keep only the product rows belonging to 2022 transactions
details_2022 = details_2022.merge(products, on="ProductID", how="left") # Add product names

top_10_products_2022 = (details_2022.groupby("ProductName")["Quantity"].sum().sort_values(ascending=False).head(15).reset_index()) # Aggregate and get the top 15 products

plt.figure(figsize=(10, 5))
sns.barplot(data=top_10_products_2022, x="ProductName", y="Quantity", errorbar=None)

plt.title("Top 15 Products Sold in 2022")
plt.xlabel("Product Name")
plt.ylabel("Total Quantity Sold")
plt.xticks(rotation=45, ha="right")
plt.show()
#-----------------------------------------------------------------------------------------------------------------------------
# Same as above but with different colors for each bar
plt.figure(figsize=(10, 5))
sns.barplot(data=top_10_products_2022, x="ProductName", y="Quantity", hue="ProductName", palette="tab20", errorbar=None)

plt.title("Top 15 Products Sold in 2022")
plt.xlabel("Product Name")
plt.ylabel("Total Quantity Sold")
plt.xticks(rotation=45, ha="right")
plt.show()
#-----------------------------------------------------------------------------------------------------------------------------
