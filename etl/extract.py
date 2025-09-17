import os
from dotenv import load_dotenv
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from sqlalchemy import create_engine, inspect
import json
import numpy as np
from pathlib import Path
from supabase import create_client, Client


load_dotenv()

login =  os.getenv('JUMPSELLER_LOGIN')
authtoken =  os.getenv('JUMPSELLER_AUTHTOKEN')

# FUNCIONES PARA EL PROCESO DE EXTRACCIÓN
# Objectivo: extraer los datos raw de la api de jumpseller para enviarlos a supabase
# JUMPSELLER -> SUPABASE

def conect2supabase():
    # Fetch variables
    USER = os.getenv("user")
    PASSWORD = os.getenv("password")
    HOST = os.getenv("host")
    PORT = os.getenv("port")
    DBNAME = os.getenv("dbname")

    # Construct the SQLAlchemy connection string
    DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

    # Create the SQLAlchemy engine
    engine = create_engine(DATABASE_URL)
    print('✅ Supabase: Conexión Exitosa')
    return engine

def obtener_datos(endpoint, login, authtoken):
    # Asegurar que la carpeta exista
    Path("data/raw").mkdir(parents=True, exist_ok=True)


    datos = []
    page = 1
    barra= tqdm(desc=f"Descargando '{endpoint}'", unit=" páginas")

    while (True):
        url = f"https://api.jumpseller.com/v1/{endpoint}.json?page={page}&limit=50"
        r = requests.get(url, auth=HTTPBasicAuth(login, authtoken))
        if r.status_code != 200:
            print(f"❌ Error al obtener {endpoint}:", r.status_code)
            break

        data = r.json()
        if not data:
            break

        datos.extend(data)
        barra.update(1) # avanza la barra en 1 unidad
        page += 1

    barra.close()


    print(f"✅ Data obtenida: {endpoint.title()}. Total páginas {page-1}\n")
    datos= pd.json_normalize(datos)
    return datos

def to_serializable(obj):
    if isinstance(obj, np.ndarray):
        return [to_serializable(x) for x in obj.tolist()]
    elif isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_serializable(x) for x in obj]
    else:
        return obj  # valores simples

def preprocessing_supabase(data_extracted):
    
    col_to_serializar=[]
    for col in data_extracted.columns:
        if data_extracted[col].apply(lambda x: isinstance(x, (np.ndarray, dict, list))).any(): 
            col_to_serializar.append(col)
    # ['product.categories', 'product.images', 'product.variants', 'product.fields]

    # print(col_to_serializar)
    if col_to_serializar: # si hay columnas 
        for col in col_to_serializar:
            # data_extracted[col]=data_extracted[col].apply(lambda fila: json.dumps(fila.tolist()) if isinstance(fila, np.ndarray) else json.dumps(fila))

            data_extracted[col] = data_extracted[col].apply( lambda fila: json.dumps(to_serializable(fila)) )
            
        print(f'Dataframe serializado. \nColumnas Serializadas: {col_to_serializar}\n')
    else:
        print('Nada que serializar')



def insert2supabase(engine, esquema, endpoint, df):
    table_name = f"{endpoint}_raw"
    full_table_name = f"{esquema}.{table_name}"

    try:
        inspector = inspect(engine)

        # 1️⃣ Verificar si la tabla ya existe en el esquema
        table_exists = inspector.has_table(table_name, schema=esquema)

        if not table_exists:
            # Crear tabla desde cero
            df.to_sql(
                table_name,
                con=engine,
                schema=esquema,
                if_exists="replace",  # replace crea la tabla si no existe
                index=False
            )
            print(f"💾 Tabla {full_table_name} creada con {len(df)} filas.")
            return

        # 2️⃣ Si la tabla ya existe, traer IDs existentes
        id_name= f"{endpoint[:-1]}.id" # ej: customer.id
    
        with engine.connect() as conn:
            query = f'SELECT "{id_name}" FROM {full_table_name}'
            existing_ids = pd.read_sql( query, conn )[id_name].tolist()

        # 3️⃣ Filtrar solo las filas nuevas
        df_new = df[~df[id_name].isin(existing_ids)]

        if not df_new.empty:
            df_new.to_sql(
                table_name,
                con=engine,
                schema=esquema,
                if_exists="append",
                index=False
            )
            print(f"💾 {len(df_new)} nuevas filas insertadas en {full_table_name}.")
        else:
            print(f"ℹ️ No hay nuevas filas para insertar en {full_table_name}.")

    except Exception as e:
        print(f"❌ Error insertando en {full_table_name}: {e}")


    # Funciones para extraer la data_raw

    # extrae todos los endpoints, ya no es necesario realizar el for fuera de la funcion
#------------------------------------------------
def extract_supabase(supabase,endpoint, esquema):
    """
    Extrae datos crudos desde una tabla en Supabase asociada a un endpoint específico.
    
    Características principales:
    - Los datos se obtienen en lotes de 1000 filas (paginación con .range).
    - Por defecto soporta hasta 5000 registros, pero se puede ampliar el rango.
    - Para el endpoint "products", se especifican las columnas exactas a consultar
      (evita traer campos innecesarios o complejos como JSON anidados).
    - Para otros endpoints, se extraen todas las columnas disponibles con '*'.
    - El proceso se detiene automáticamente cuando no existen más filas en el rango.
    - Une todos los lotes extraídos en un único DataFrame de Pandas.
    
    Input: endpoint(str)
    Nombre del endpoint a consultar (ejemplo: "products", "orders", "customers").
    
    Output: Un DataFrame con todos los registros obtenidos de la tabla `{endpoint}_raw`.
    """
    name_table= f"{endpoint}_{esquema}"
    rango= [0,1000,2000,3000,4000]
    # evaluar o almacenar mientras exista rango en la tabla si lanza el error entonces parar

    parts_table=[]

    if esquema=='raw' and endpoint=='products': # columnas obtenidas luego de query en SQL EDITOR
        select_query=' "product.id" , "product.name" , "product.page_title" , "product.description" , "product.meta_description" , "product.price" , "product.cost_per_item" , "product.compare_at_price" , "product.weight" , "product.stock" , "product.stock_unlimited" , "product.stock_threshold" , "product.stock_notification" , "product.sku" , "product.brand" , "product.barcode" , "product.featured" , "product.reviews_enabled" , "product.status" , "product.shipping_required" , "product.type" , "product.days_to_expire" , "product.created_at" , "product.updated_at" , "product.package_format" , "product.length" , "product.width" , "product.height" , "product.diameter" , "product.google_product_category" , "product.images" , "product.variants" , "product.fields" , "product.permalink" , "product.discount" , "product.currency" '
        # select_query=products_column
    else:
        select_query="*"

    # Obtener data de Supabase por partes
    for valor in rango:
        table_chunk = ( 
        supabase.table(name_table)
        .select(select_query)
        .range(valor,valor+999) # 0,999 , lo mismo= rango[ind]
        .execute()
            )
        if len(table_chunk.data): # si existe la tabla en ese rango
            parts_table.append(table_chunk.data)
        else:
            break

    # Juntar todas las listas en una sola lista
    completed_table =[]
    for chunk in parts_table:
        completed_table+= chunk
    
    print(f'✅ Extracción correcta realizada para {name_table}')
    print(f"Filas: {len(completed_table)}\n")

    return pd.DataFrame(completed_table)


# Traer la información por lotes
# consideración hasta 5000 mil registros
    
# def extract_supabase_raw(endpoint): # cambiar despues a endpoints(lista)
#     name_table= f"{endpoint}_raw"
#     rango= [0,1000,2000,3000,4000]
#     # evaluar o almacenar mientras exista rango en la tabla si lanza el error entonces parar

#     parts_table=[]

#     if endpoint=='products': # columnas obtenidas luego de query en SQL EDITOR
#         products_column=' "product.id" , "product.name" , "product.page_title" , "product.description" , "product.meta_description" , "product.price" , "product.cost_per_item" , "product.compare_at_price" , "product.weight" , "product.stock" , "product.stock_unlimited" , "product.stock_threshold" , "product.stock_notification" , "product.sku" , "product.brand" , "product.barcode" , "product.featured" , "product.reviews_enabled" , "product.status" , "product.shipping_required" , "product.type" , "product.days_to_expire" , "product.created_at" , "product.updated_at" , "product.package_format" , "product.length" , "product.width" , "product.height" , "product.diameter" , "product.google_product_category" , "product.images" , "product.variants" , "product.fields" , "product.permalink" , "product.discount" , "product.currency" '
#         select_query=products_column
#     else:
#         select_query="*"

#     # Obtener data de Supabase por partes
#     for ind, valor in enumerate(rango):
#         table_chunk = ( 
#         supabase.table(name_table)
#         .select(select_query)
#         .range(valor,valor+999) # 0,999 , lo mismo= rango[ind]
#         .execute()
#             )
#         if len(table_chunk.data): # si existe la tabla en ese rango
#             parts_table.append(table_chunk.data)
#         else:
#             break

#     completed_table =[]

#     # Juntar todas las listas en una sola lista
#     for i in range(len(parts_table)):
#         completed_table+= parts_table[i]
    
#     print(f'✅ Extracción correcta realizada para {name_table}')
#     print(f"Filas: {len(completed_table)}\n")

#     return pd.DataFrame(completed_table)