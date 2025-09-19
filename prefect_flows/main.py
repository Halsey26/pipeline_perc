import os
from dotenv import load_dotenv
from prefect import flow, task
from pipeline_perc.etl.extract import obtener_datos, conect2supabase,preprocessing_supabase, extract_supabase
from pipeline_perc.etl.transform import transform_customers, transform_orders, transform_products, create_orders_products
from pipeline_perc.etl.load import insert2supabase
from tqdm import tqdm
from supabase import create_client, Client

# para ejecutar en terminal de venv: python -m pipeline_perc.prefect_flows.main
load_dotenv()
login =  os.getenv('JUMPSELLER_LOGIN')
authtoken =  os.getenv('JUMPSELLER_AUTHTOKEN')

# cliente supabase
url= os.environ.get("SUPABASE_URL")
key= os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

endpoints=[ "products","customers","orders"]

@task
def extract(endpoints):

    # Crear carpeta data/raw si no existe
    # Path("data/raw").mkdir(parents=True, exist_ok=True)
    
    engine_supabase=conect2supabase() # en local, en producción usar Blocks
    with engine_supabase.connect() as connection:

        # Obtiene los datos
        dataframes={}
        for endpoint in endpoints:
            dataframes[f'df_{endpoint}']= obtener_datos(endpoint, login, authtoken)

        # Preprocesa para serializar
        for key, df in dataframes.items():
            print(key)
            preprocessing_supabase(df)

        # Inserción en data raw - Supabase: Esquema Raw
        esquema= 'raw'
        for endpoint in tqdm(endpoints, desc='Insertando en Supabase'):
            df= dataframes[f'df_{endpoint}']
            insert2supabase(engine_supabase, esquema, endpoint, df)

@task
def transform(endpoints, supabase_cliente):
    #1. Extracción data_raw de Supabase
    esquema='raw'
    df_raw= {}
    for endpoint in tqdm(endpoints):
        df_raw[f'{endpoint}_raw']= extract_supabase(supabase= supabase_cliente,endpoint=endpoint, esquema=esquema) 
    #2. Funcions de Limpieza, normalizacion para cada tabla
    dataframes_clean ={}
    for endpoint in endpoints:
        funcion= globals().get(f"transform_{endpoint}")
        dataframes_clean[f"{endpoint}_clean"] = funcion(df_raw[f'{endpoint}_raw'])  #ej: transform_orders(df_raw['orders_raw'])

    dataframes_clean["orders_products_clean"] = create_orders_products(dataframes_clean["orders_clean"])
    return dataframes_clean

@task
def load(dataframes_clean):
    engine_supabase=conect2supabase() # en local, en producción usar Blocks
    with engine_supabase.connect() as connection:
         # Inserción de data clean - Supabase: Esquema Clean
        esquema= 'clean'
        endpoints= [ "products","customers","orders","orders_products"]

        # for key, df in tqdm(dataframes_clean.items(), desc= 'Insertando en Supabase'):
        for endpoint in tqdm(endpoints, desc='Insertando en Supabase'):
            df= dataframes_clean[f'{endpoint}_clean']
            insert2supabase(engine_supabase, esquema, endpoint, df)


@flow
def main_flow():
    extract(endpoints)
    df= transform(endpoints,supabase)
    load(df)


if __name__ == "__main__":
    main_flow()

# ejecutar: python -m prefect_flows.main

