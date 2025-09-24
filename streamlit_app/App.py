# APP LOGÍCA E INTEGRACIÓN DE LAS PÁGINAS
import sys, os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
# agregar la raíz del repo al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import streamlit as st
import pandas as pd
import plotly.express as px
# from pipeline_perc.etl.extract import extract_supabase (local)
from etl.extract import extract_supabase #(deploy)
from supabase import create_client, Client
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from estilos import load_css, colores
import base64
# traer las páginas
from Inicio import *


# CONEXION Y EXTRACCION
load_dotenv()
url= os.environ.get("SUPABASE_URL")
key= os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

endpoints= [ "products","customers","orders","orders_products"]
esquema='clean'
# Solo cargar si no existe en sesión
if "df_clean" not in st.session_state:
    df_clean = {}
    for endpoint in endpoints:
        df_clean[f"{endpoint}_clean"] = extract_supabase(supabase,endpoint=endpoint, esquema=esquema)
    st.session_state.df_clean = df_clean
else:
    df_clean = st.session_state.df_clean


# --- Configuración de la página ---
st.set_page_config(
    page_title="Business Analytics - Kreadores PRO",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="📷"
)

load_css(colores)

kreadores_header()

# Navegación con colores de Kreadores
page = st.sidebar.radio(
    "🎯 SELECCIONA:",
    ("Inicio", "Clientes", "Órdenes", "Productos"),
    index=0
)



