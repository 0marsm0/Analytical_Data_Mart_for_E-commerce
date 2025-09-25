import pandas as pd
from pathlib import Path


def load_raw_data(path, file_list):
    data = {}

    for file in file_list:
        df = pd.read_csv(path / file)
        data[file] = df

    return data


def preprocess_data(data_dict):

    data_change_dict = {
        "olist_order_items_dataset.csv": {
            "order_item_id": "int",
            "price": "float",
            "freight_value": "float",
        },
        "olist_order_payments_dataset.csv": {
            "payment_sequential": "int",
            "payment_installments": "int",
            "payment_value": "float",
        },
        "olist_orders_dataset.csv": {
            "order_purchase_timestamp": "datetime",
            "order_approved_at": "datetime",
            "order_delivered_carrier_date": "datetime",
            "order_delivered_customer_date": "datetime",
            "order_estimated_delivery_date": "datetime",
        },
        "olist_products_dataset.csv": {
            "product_name_lenght": "int",
            "product_description_lenght": "int",
            "product_photos_qty": "int",
            "product_weight_g": "int",
            "product_length_cm": "int",
            "product_height_cm": "int",
            "product_width_cm": "int",
        },
    }

    for key in data_change_dict.keys():
        for column, value in data_change_dict[key].items():
            match value:
                case "int":
                    data_dict[key][column] = data_dict[key][column].astype("Int64")
                case "float":
                    data_dict[key][column] = data_dict[key][column].astype(float)
                case "datetime":
                    data_dict[key][column] = pd.to_datetime(data_dict[key][column])

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
