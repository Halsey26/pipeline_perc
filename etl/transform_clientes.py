import json
import re
import pandas as pd

def normalizar_texto_localidad(texto):
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
    "nunoa ": "Ñuñoa",
    "penalolen": "Peñalolén",
    "penaflor": "Peñaflor",
    "talcahuano ": "Talcahuano",
    "concepcion": "Concepción",
    "concepcion ": "Concepción",
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
        df['customer.shipping_addresses'].apply(parse_direcciones)
    )

    df['cliente_ciudad'] = df['cliente_ciudad'].apply(lambda x: normalizar_texto_localidad(x))


    df.drop(columns=['customer.shipping_addresses'], inplace=True)
    
    # --- 3. Renombrado de columnas
    df.columns = [col.replace('customer.', 'cliente_') for col in df.columns]
    
    # --- 4. Tratamiento de nulos
    # Flags de missing

    # return df.columns
    for col in ['cliente_ciudad', 'cliente_municipalidad', 'cliente_accepts_marketing']:
        df[f'{col}_missing'] = df[col].isna().astype(int)
    
    # Columnas categóricas → reemplazo por "Sin especificar"
    cols_categoricas = ['cliente_status', 'cliente_ciudad', 'cliente_municipalidad', 'cliente_pais']
    df[cols_categoricas] = df[cols_categoricas].fillna('Sin especificar')

    df['cliente_accepts_marketing'] = df['cliente_accepts_marketing'].fillna(False)
    
    # --- 5. Versión categórica de accepts_marketing
    df['cliente_marketing_cat'] = df['cliente_accepts_marketing'].map({
        True: 'Si',
        False: 'No'
    })
    
    # Resultado final
    customers_clean = df.copy()
    return customers_clean

