import pandas as pd
from pathlib import Path


def load_raw_data(path, file_list):
    data = {}

    for file in file_list:
        df = pd.read_csv(path / file)
        data[file] = df

    return data


def preprocess_data(data_dict):

    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    orders_df_name = "olist_orders_dataset.csv"

    if orders_df_name in data_dict:
        print(f"Processing dates for {orders_df_name}...")
        for col_name in date_columns:
            data_dict[orders_df_name][col_name] = pd.to_datetime(
                data_dict[orders_df_name][col_name]
            )
    else:
        print(f"Warning: DataFrame {orders_df_name} not found.")

    return data_dict


if __name__ == "__main__":

    DATA_PATH = Path(__file__).parent.parent / "data/"

    CSV_FILES = [
        "olist_products_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_sellers_dataset.csv",
    ]

    raw_df_list = load_raw_data(DATA_PATH, CSV_FILES)

    clean_df_list = preprocess_data(raw_df_list)

    clean_df_list["olist_orders_dataset.csv"].info()
