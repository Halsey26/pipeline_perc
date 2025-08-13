import os
from dotenv import load_dotenv
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm


load_dotenv()

login =  os.getenv('JUMPSELLER_LOGIN')
authtoken =  os.getenv('JUMPSELLER_AUTHTOKEN')


def obtener_datos(endpoint, login, authtoken):
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


    print(f"✅ Data obtenida: {endpoint.title()}. Total páginas {page-1}")
    datos= pd.json_normalize(datos)
    datos.to_parquet(f"data/raw/{endpoint}_raw.parquet", index=False)
    return datos




