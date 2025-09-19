
import pandas as pd
from sqlalchemy import inspect

def insert2supabase(engine, esquema, endpoint, df):
    table_name = f"{endpoint}_{esquema}"
    full_table_name = f"{esquema}.{table_name}"

    try:
        inspector = inspect(engine)

        # Verificar si la tabla ya existe en el esquema
        table_exists = inspector.has_table(table_name, schema=esquema)

        #☑️ SI no existe, Crear tabla desde cero
        if not table_exists:
            df.to_sql(
                table_name,
                con=engine,
                schema=esquema,
                if_exists="replace",  # replace crea la tabla si no existe
                index=False
            )
            print(f"💾 Tabla {full_table_name} creada con {len(df)} filas.")
            return

        #☑️ SI la tabla existe, se añade las nuevas filas

        # 1️⃣ Obtiene las columnas de la tabla en Supabase
        with engine.connect() as conn:
            query=f""" 
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = '{esquema}'
                AND table_name = '{table_name}';
            """
            columnas_actuales= pd.read_sql(query, conn)['column_name'].tolist()

        # Filtrar df para no añadir columnas nuevas
        df= df[[c for c in df.columns if c in columnas_actuales]]


        # 2️⃣ Si la tabla ya existe, traer IDs existentes
        if esquema=='raw':
            id_name= f"{endpoint[:-1]}.id" # tengo que limpiar la ultima letra
        elif esquema=='clean':
            ID_MAP = {
                "products": "id_producto",
                "customers": "id_cliente",
                "orders": "id_orden",
                "orders_products": "id_orden"  # o (id_orden, id_producto) si necesitas PK compuesta
            }
            id_name=ID_MAP.get(endpoint)
    
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
