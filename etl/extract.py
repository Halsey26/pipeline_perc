import os
from dotenv import load_dotenv
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from sqlalchemy import create_engine
import json
import numpy as np
from pathlib import Path

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


def insert2supabase(engine,esquema, endpoint, df):
    try: 
        table_name= f'{endpoint}_raw'
        df.to_sql(
            table_name,
            con=engine,
            schema=esquema, # el esquema debe estar creado previamente
            if_exists='append', #append para no soreescrbbir, ()
            index=False
        )
        print(f'💾 Creación exitosa de la tabla {table_name} en el esquema {esquema}.\n')
    except Exception as e:
        print(f"Error para crear la tabla {table_name}: {e}")

