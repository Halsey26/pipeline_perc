import os
from supabase import create_client, Client
from dotenv import load_dotenv
from tqdm import tqdm
import re
import pandas as pd
import json
from rapidfuzz import process
import numpy as np


# ----------- CUSTOMERS
def normalizar_texto_localidad(texto ):
    """
    Normaliza nombres de ciudades o municipalidades.
    
    - texto: string a normalizar
    - reemplazos: diccionario de equivalencias
    """
    reemplazos= {
    "santiago centro": "Santiago",
    "santiago de chile": "Santiago",
    "santiago  ": "Santiago",
    "ssntiago": "Santiago",
    "santiago": "Santiago",
    "SANTIAGO": "Santiago",
    "vina del mar": "Viña del Mar",
    "viña del mar ": "Viña del Mar",
    "valparaiso": "Valparaíso",
    "valparaiso ": "Valparaíso",
    "puerto montt": "Puerto Montt",
    "punta arenas": "Punta Arenas",
    "chillan": "Chillán",
    "chillan viejo": "Chillán Viejo",
    "temuco ": "Temuco",
    "temuco": "Temuco",
    "arica": "Arica",
    "antofagasta": "Antofagasta",
    "rancagua": "Rancagua",
    "curico": "Curicó",
    "san pedro de la paz": "San Pedro de la Paz",
    "los angeles": "Los Ángeles",
    "los andes": "Los Andes",
    "la serena": "La Serena",
    "valdivia": "Valdivia",
    "providencia ": "Providencia",
    "san miguel ": "San Miguel",
    "nunoa": "Ñuñoa",
    "penalolen": "Peñalolén",
    "penaflor": "Peñaflor",
    "talcahuano ": "Talcahuano",
    "concepcion": "Concepción"
    }

    if pd.isna(texto):
        return "Sin especificar"
    
    # --- 1. Limpieza básica
    t = texto.strip().lower()
    
    # --- 2. Diccionario de equivalencias conocidas
    if t in reemplazos:
        return reemplazos[t]
    
    # --- 3. Correcciones genéricas
    t = re.sub(r'\s+', ' ', t)   # quitar dobles espacios
    t = t.title()                # Capitalización estándar
    
    # --- 4. Casos inválidos genéricos
    if t in ["Asd", "Chile", "Guamuchil", "Sin Especificar"]:
        return "Sin especificar"
    
    return t

def transform_customers(df_raw):
    """
    Recibe el df raw de customers y devuelve un df limpio (customers_clean).
    
    Pasos aplicados:
    1. Selección de columnas relevantes
    2. Parseo de direcciones (ciudad, municipalidad, pais)
    3. Renombrado con prefijo 'cliente_'
    4. Tratamiento de nulos y creación de flags
    5. Generación de columna categórica para accepts_marketing
    """
    
    # --- 1. Selección de columnas relevantes
    columnas_filtro = [
        'customer.id',
        'customer.fullname',
        'customer.status',
        'customer.accepts_marketing',
        'customer.shipping_addresses'
    ]
    df = df_raw[columnas_filtro].copy()

    # --- Renombrado
    df = df.rename(columns={
        'customer.id': 'id_cliente',
        'customer.fullname': 'cliente_nombre',
        'customer.status':'status',
        'customer.accepts_marketing': 'acepta_marketing',
        'customer.shipping_addresses': 'direccion_envio'
    })

    
    # --- 2. Parseo de direcciones
    def parse_direcciones(fila):
        try:
            fila_parseada = json.loads(fila)
            if not fila_parseada:  # si está vacío
                return pd.Series([None, None, 'Chile'])
            
            dicc = fila_parseada[0]  # primer elemento (diccionario)
            return pd.Series([
                dicc.get('city'),
                dicc.get('municipality'),
                'Chile'
            ])
        except Exception:
            return pd.Series([None, None, 'Chile'])
    
    df[['cliente_ciudad', 'cliente_municipalidad', 'cliente_pais']] = (
        df['direccion_envio'].apply(parse_direcciones)
    )

    df['cliente_ciudad'] = df['cliente_ciudad'].apply(lambda x: normalizar_texto_localidad(x))


    df.drop(columns=['direccion_envio'], inplace=True)
    
    # --- 3. Renombrado de columnas
    # df.columns = [col.replace('customer.', 'cliente_') for col in df.columns]
    
    # --- 4. Tratamiento de nulos
    # Flags de missing

    # return df.columns
    for col in ['cliente_ciudad', 'cliente_municipalidad', 'acepta_marketing']:
        df[f'{col}_missing'] = (df[col].isna() | (df[col]== 'Sin especificar') ).astype(int)
    
    # Columnas categóricas → reemplazo por "Sin especificar"
    cols_categoricas = ['status', 'cliente_ciudad', 'cliente_municipalidad', 'cliente_pais']
    df[cols_categoricas] = df[cols_categoricas].fillna('Sin especificar')

    df['acepta_marketing'] = df['acepta_marketing'].fillna(False)
    
    # --- 5. Versión categórica de accepts_marketing
    df['marketing_cat'] = df['acepta_marketing'].map({
        True: 'Si',
        False: 'No'
    })
    
    # Resultado final
    customers_clean = df.copy()
    return customers_clean

# ------------------- PRODUCTS

def normalizar_marca(df, col="marca"): 
    """
    Normaliza la columna de marcas de productos:
    - Convierte valores nulos a 'Sin especificar'
    - Corrige errores comunes de tipeo con fuzzy matching
    """

    # ✅ Marcas más comunes
    # devuelve la lista con solo valores reales de marcas, verifica que esas marcas realmente existan, si hay Sony y SONY, escoje solo uno Sony y de manera similares, si hay variantes de una misma marca. 
    # de igual manera incluye marcas comunes que no estén en la lista
    marcas_validas = ['Accsoon', 'Benro', 'Blackmagic Design', 'Blik', 'Boya', 'Canon',
    'DJI', 'Feelworld', 'Fujifilm', 'Godox', 'Hebikuo', 'Hercules',
    'Hollyland', 'Hosa Technology', 'Iluminus', 'Joby', 'KyF', 'Lexar',
    'Lowepro', 'Maono', 'Manfrotto', 'Nanlite', 'Neewer', 'NewLine', 
    'Nikon', 'Nisi', 'NiceFoto', 'Olympus', 'Panasonic', 'Pentax',
    'Rode', 'Samsung', 'SanDisk', 'Saramonic', 'Seagate', 'Seetec',
    'Sigma', 'Sin especificar', 'Sirui', 'Sony', 'Tamron','Tascam', 'Tenba',
    'Tether Tools', 'Triopo', 'Ulanzi', 'Vijim', 'Visico', 'Viltrox',
    'Zhiyun', 'Zoom']


    # ✅ Función de limpieza
    def limpiar_empresa(x):
        if pd.isna(x):
            return "Sin especificar"
        x = str(x).strip()

        # si el valor parece numérico → error → lo tratamos como desconocido
        if x.replace(".", "").isdigit():
            return "Sin especificar"

        return x

    # ✅ Función de normalización con fuzzy
    def normalizar(x):
        x = limpiar_empresa(x)

        mejor = process.extractOne(x, marcas_validas, score_cutoff=30)
        return mejor[0] if mejor else "Sin especificar"

    # ✅ Aplicar la transformación
    df["marca"] = df[col].apply(normalizar)

    return df
#probar para una prueba producotscopy= productos_clean.copy()

def transform_products(df):
    columnas_filtro= ['product.id',
    'product.name',
    'product.price',
    'product.stock',
    'product.stock_threshold',
    'product.stock_notification',
    'product.brand',
    'product.reviews_enabled',
    'product.status',
    'product.created_at',
    'product.updated_at',
    'product.currency']

    # --- 1. Filtro de columnas
    df = df[columnas_filtro].copy()
    
    # --- Renombrado
    df = df.rename(columns={
        'product.id':'id_producto',
        'product.name':'descripcion',
        'product.price':'precio',
        'product.stock': 'stock',
        'product.stock_threshold': 'umbral_stock',
        'product.stock_notification': 'notificacion_stock',
        'product.brand': 'marca',
        'product.reviews_enabled': 'reseñas_habilitadas',
        'product.status': 'estado',
        'product.created_at': 'fecha_creacion',
        'product.updated_at': 'fecha_actualizacion',
        'product.currency': 'moneda'
    })
    # ---
    df =normalizar_marca(df)

    # --- 3. Relleno de categóricas
    cols_categoricas = ['descripcion', 'marca', 'estado', 'moneda']
    df[cols_categoricas] = df[cols_categoricas].fillna('Sin especificar')

    # --- 3. Manejo de fechas
    for col in ['fecha_creacion', 'fecha_actualizacion']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date  # solo YYYY-MM-DD

    # --- 4. Manejo de precio
    df['precio']=df['precio'].fillna(0)

    # Resultado final
    df_clean = df.copy()
    return df_clean

# ---------------- ORDERS


def normalizar_empresas_envio(df, col="empresa_envio"): #empresa_envio
    """
    Normaliza la columna de empresas de envío:
    - Convierte valores numéricos a 'Sin especificar'
    - Corrige errores comunes de tipeo con fuzzy matching
    - Devuelve una nueva columna 'empresa_envio_normalizada'
    """

    # ✅ Catálogo oficial de empresas de envío
    empresas_validas = [
        "Envío Express",
        "Blue Express",
        "BluExpress Express",
        "BluExpress Priority",
        "Estándar a domicilio",
        "Estándar a sucursal",
        "Prioritario",
        "Prioritario a domicilio",
        "Prioritario a sucursal",
        "Correo Ordinario",
        "Sin especificar"
    ]

    # ✅ Función de limpieza
    def limpiar_empresa(x):
        if pd.isna(x):
            return "Sin especificar"
        x = str(x).strip()

        # si el valor parece numérico → error → lo tratamos como desconocido
        if x.replace(".", "").isdigit():
            return "Sin especificar"

        return x

    # ✅ Función de normalización con fuzzy
    def normalizar_empresa(x):
        x = limpiar_empresa(x)

        mejor = process.extractOne(x, empresas_validas, score_cutoff=30)
        return mejor[0] if mejor else "Sin especificar"

    # ✅ Aplicar la transformación
    df["empresa_envio"] = df[col].apply(normalizar_empresa)

    return df


def transform_orders(df_raw):
    """
    Recibe el df raw de orders y devuelve orders_clean.
    Aplica: selección, renombrado, parseo, mapeo, tratamiento de nulos y tipos.
    """
    
    columnas_filtro = [
        'order.id',
        'order.created_at',
        'order.currency',
        'order.total',
        'order.fulfillment_status',
        'order.shipping_method_name',
        'order.customer.id',
        'order.shipping_address.region',
        'order.shipping_address.country',
        'order.shipping_address.municipality',
        'order.products',
        'order.status',
        'order.shipment_status'
    ]
    
    df = df_raw[columnas_filtro].copy()
    
    # --- Renombrado
    df = df.rename(columns={
        'order.id': 'id_orden',
        'order.created_at': 'fecha_creacion',
        'order.currency': 'moneda',
        'order.total': 'precio_total',
        'order.fulfillment_status': 'estado_cumplimiento',
        'order.shipping_method_name': 'empresa_envio',
        'order.customer.id': 'id_cliente',
        'order.shipping_address.region': 'region_envio',
        'order.shipping_address.country': 'pais_envio',
        'order.shipping_address.municipality': 'municipalidad_envio',
        'order.products': 'productos',
        'order.status': 'estado_orden',
        'order.shipment_status': 'estado_envio'
    })
    
    # --- Tipos y nulos
    df['id_orden'] = pd.to_numeric(df['id_orden'], errors='coerce')
    df = df.drop_duplicates(subset='id_orden')
    
    df['fecha_creacion'] = pd.to_datetime(df['fecha_creacion'], errors='coerce').dt.date
    # df['fecha_completacion'] = pd.to_datetime(df['fecha_completacion'], errors='coerce').dt.date
    
    df['moneda'] = df['moneda'].fillna('CLP')
    df['precio_total'] = pd.to_numeric(df['precio_total'], errors='coerce').fillna(0).astype(int)
    

    df['id_cliente'] = df['id_cliente'].fillna(0).astype(int) #generar una columna si id_cliente tiene valor diferente a 0 'Registrado' sino 'No registrado'
    df['estado_cliente']= np.where(df['id_cliente'] != 0, 'Registrado', 'No Registrado')
    df['pais_envio'] = df['pais_envio'].fillna('Chile')
    

    # --- Parsear productos (extraer ids de productos)
    def extraer_ids(productos):
        try:
            items = json.loads(productos) if isinstance(productos, str) else productos
            if isinstance(items, list):
                return ";".join(str(p.get("id")) for p in items if p.get("id"))
        except Exception:
            return None
    df['productos_ids'] = df['productos'].apply(extraer_ids)
    df.drop(columns=['productos'], inplace=True)

    df['estado_cumplimiento'] = df['estado_cumplimiento'].map({
        'unfulfilled': 'No cumplido',
        'fulfilled': 'Cumplido' })
    
    df['estado_orden'] = df['estado_orden'].map({
        "Abandoned": "Abandonada",
        "Paid": "Pagada",
        "Canceled": "Cancelada",
        "Created": "Creada",
        "Pending Payment": "Pendiente de pago" })
    
    df['estado_envio'] = df['estado_envio'].map({
        "No Procesado": "No procesado",
        "Entregado": "Entregado",
        "Solicitado": "Solicitado",
        "No Aplicable": "No aplicable",
        "En Tránsito": "En tránsito"  })

    # nulos
    cols = ['estado_cumplimiento', 'region_envio', 'municipalidad_envio','estado_orden','estado_envio'] #empresas_envio
    df[cols] = df[cols].fillna('Sin especificar')

    # normaliza empresas de envio
    df = normalizar_empresas_envio(df)
    
    
    return df


# ---------- NUEVA TABLA: ORDERS - PRODUCT

def create_orders_products(df_orders, col_order="id_orden", col_products="productos_ids"):
    """
    Crea una tabla de relación orders_products a partir de un dataframe de órdenes.
    df_orders: DataFrame con al menos las columnas id_order y id_producto (string con ';' separados).
    col_order: nombre de la columna con el id de la orden.
    col_products: nombre de la columna con los ids de productos (str separados por ';').
    """
    
    # copiar para no alterar df original
    df_rel = df_orders[[col_order, col_products]].copy()
    
    # separar productos en listas
    df_rel[col_products] = df_rel[col_products].astype(str).str.split(";")
    
    # "explotar" listas en filas
    df_rel = df_rel.explode(col_products).reset_index(drop=True)
    
    # convertir id_producto a entero si es posible
    df_rel[col_products] = pd.to_numeric(df_rel[col_products], errors="coerce").astype("Int64")
    
    # renombrar columnas para consistencia
    df_rel = df_rel.rename(columns={
        col_order: "id_orden",
        col_products: "id_producto"
    })
    
    return df_rel
