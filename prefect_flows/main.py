import os
from dotenv import load_dotenv
from prefect import flow, task
from etl.extract import obtener_datos
from pathlib import Path


login =  os.getenv('JUMPSELLER_LOGIN')
authtoken =  os.getenv('JUMPSELLER_AUTHTOKEN')

@task
def extract():
    # Crear carpeta data/raw si no existe
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    df_products = obtener_datos("products", login, authtoken)
    df_customers = obtener_datos("customers", login, authtoken)
    df_orders = obtener_datos("orders", login, authtoken)

    df_products.to_parquet("data/raw/products_raw.parquet", index=False)
    df_customers.to_parquet("data/raw/customers_raw.parquet", index=False)
    df_orders.to_parquet("data/raw/orders_raw.parquet", index=False)

    return df_products, df_customers, df_orders

@flow
def main_flow():
    productos, customers, orders= extract()

if __name__ == "__main__":
    main_flow()

# ejecutar: python -m prefect_flows.main

