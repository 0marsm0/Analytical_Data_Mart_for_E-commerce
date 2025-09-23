import pandas as pd


DATA_PATH = "../data/"

csv_files = [
    "olist_products_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_sellers_dataset.csv",
]

data = {}

for file in csv_files:
    df = pd.read_csv(DATA_PATH + file)
    data[file] = df

print(data.keys())
