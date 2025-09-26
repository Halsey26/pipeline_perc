# Variales de entorno

Las variables de entorno utilizadas son:


## Para la extracción
Usado en [prefect_flows/main.py](https://github.com/Halsey26/pipeline_perc/blob/main/prefect_flows/main.py), para conexión con API Jumpseller.
- JUMPSELLER_LOGIN = 
- JUMPSELLER_AUTHTOKEN = 

## Variables del proyecto creado en Supabase
Usado en [etl/extract.py](https://github.com/Halsey26/pipeline_perc/blob/main/etl/extract.py), función `conect2supabase()`

- user=postgres 
- password= 
- host= 
- port= 
- dbname= 

## Para crear cliente con Supabase
- SUPABASE_URL=https://m
- SUPABASE_KEY=eyJh...

## para el deploy
PYTHON_VERSION=3.11.9



