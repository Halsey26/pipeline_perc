import os
from dotenv import load_dotenv
from prefect import flow, task
from pipeline_perc.etl.extract import obtener_datos, conect2supabase,preprocessing_supabase,insert2supabase
from pathlib import Path
from sqlalchemy import create_engine
from tqdm import tqdm
# para ejecutar en terminal de venv: python -m pipeline_perc.prefect_flows.main

login =  os.getenv('JUMPSELLER_LOGIN')
authtoken =  os.getenv('JUMPSELLER_AUTHTOKEN')


@task
def extract():
    # Crear carpeta data/raw si no existe
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    
    engine_supabase=conect2supabase() # en local, en producción usar Blocks
    with engine_supabase.connect() as connection:
        endpoints=[ "products","customers","orders"]

        # Obtiene los datos
        dataframes={}
        for endpoint in endpoints:
            dataframes[f'df_{endpoint}']= obtener_datos(endpoint, login, authtoken)

        # Preprocesa para serializar
        for key, df in dataframes.items():
            print(key)
            preprocessing_supabase(df)

        # Inserción en Supabase de data raw
        esquema= 'raw'
        for key, df in tqdm(dataframes.items(), desc= 'Insertando en Supabase'):
            for endpoint in endpoints:
                if key.endswith(endpoint):
                    insert2supabase(engine_supabase,esquema, endpoint, df)

    #     df_products = obtener_datos("products", login, authtoken)
    #     df_customers = obtener_datos("customers", login, authtoken)
    #     df_orders = obtener_datos("orders", login, authtoken)

    #     df_products.to_parquet("data/raw/products_raw.parquet", index=False)
    #     df_customers.to_parquet("data/raw/customers_raw.parquet", index=False)
    #     df_orders.to_parquet("data/raw/orders_raw.parquet", index=False)

    # return df_products, df_customers, df_orders

@flow
def main_flow():
    extract()
    # productos, customers, orders= extract()

if __name__ == "__main__":
    main_flow()

# ejecutar: python -m prefect_flows.main

