import pandas as pd
from pathlib import Path
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()


def load_raw_data(path, file_list):
    data = {}

    for file in file_list:
        try:
            df = pd.read_csv(path / file)
            data[file] = df
            print(f"{file} has been successfully added to 'data' dictionary")
        except Exception as err:
            print(f"ERROR: Failed to read csv. Reason: {err}")

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


def load_to_postgres(data_dict):
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    db = os.getenv("POSTGRES_DB")

    try:
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db}")

        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data"))
            conn.commit()

        for key, value in data_dict.items():

            try:
                table_name = key.replace("olist_", "").replace("_dataset.csv", "")
                value.to_sql(
                    name=table_name,
                    con=engine,
                    schema="raw_data",
                    if_exists="replace",
                    index=False,
                )
                print(
                    f"Table {table_name} has been successfully added to schema raw_data"
                )
            except Exception as err:
                print(f"ERROR: Failed to load table '{table_name}'. Reason: {err}")

    except SQLAlchemyError as err:
        print(f"ERROR:Failed to connect to database. Reason: {err}")


if __name__ == "__main__":

    DATA_PATH = Path(__file__).parent / "data/"

    CSV_FILES = [
        "olist_products_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_sellers_dataset.csv",
    ]

    raw_df_dict = load_raw_data(DATA_PATH, CSV_FILES)
    clean_df_dict = preprocess_data(raw_df_dict)
    load_to_postgres(clean_df_dict)
