import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st
import pandas as pd
import plotly.express as px
from pipeline_perc.etl.extract import extract_supabase
from supabase import create_client, Client
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
# Ejecutar: python -m pipeline_perc.streamlit_app.dashboard
# streamlit run pipeline_perc/streamlit_app/Inicio.py

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


# ---- CONFIG ----
st.set_page_config(page_title="Kreadores PRO Dashboard", layout="wide")
st.title("📊 Kreadores PRO - Dashboard")
st.sidebar.header("Filtros")

# ---- DATA ----
df_clean = st.session_state.df_clean

customers = df_clean['customers_clean']
orders = df_clean['orders_clean']
products = df_clean['products_clean']
orders_products = df_clean['orders_products_clean']

# ---- KPIs ----
total_clientes = customers['id_cliente'].nunique()
total_ordenes = orders['id_orden'].nunique()
ingresos_totales = orders['precio_total'].sum()

# Ticket promedio (ingreso por orden)
ticket_promedio = ingresos_totales / total_ordenes if total_ordenes > 0 else 0

# Gasto Promedio por Cliente 
gasto_promedio_cliente = ingresos_totales / total_clientes

# Clientes activos
clientes_activos = orders['id_cliente'].nunique()

# Clientes con más de 1 compra
compras_por_cliente = orders.groupby("id_cliente")['id_orden'].count()
clientes_recompra = (compras_por_cliente > 1).sum()
tasa_recompra = clientes_recompra / total_clientes * 100


col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("👥 Clientes", total_clientes)
col2.metric("🛒 Órdenes", total_ordenes)
col3.metric("💰 Ingresos Totales (CLP)", f"${ingresos_totales:,.2f}")
col4.metric("🎟️ Ticket Promedio", f"${ticket_promedio:,.2f}")
col5.metric("📈 Gasto Promedio por Cliente", f"${gasto_promedio_cliente:,.2f}")
col6.metric("💸 Propensión a Recompra", f"{tasa_recompra:,.2f}%")

# ---- VISUALIZACIONES ----

# 1. Ventas en el tiempo
df_orders = orders.copy()
df_orders['fecha_creacion'] = pd.to_datetime(df_orders['fecha_creacion'])
ventas_tiempo = df_orders.groupby(df_orders['fecha_creacion'].dt.to_period("M"))['precio_total'].sum().reset_index()
ventas_tiempo['fecha_creacion'] = ventas_tiempo['fecha_creacion'].astype(str)

fig_time = px.line(
    ventas_tiempo, x='fecha_creacion', y='precio_total',
    title="📅 Ventas Mensuales",
    markers=True,
    labels={"fecha_creacion": "Fecha", "precio_total": "Ingresos (CLP)"}
)
st.plotly_chart(fig_time, use_container_width=True)

# 2. Ventas por país (aunque sea redundante en Chile, útil si se expande)
ventas_pais = orders.groupby('pais_envio')['precio_total'].sum().reset_index()
fig_pais = px.bar(
    ventas_pais, x='pais_envio', y='precio_total',
    title="🌍 Ventas por País",
    text_auto=True,
    labels={"pais_envio": "País", "precio_total": "Ingresos (CLP)"}
)
st.plotly_chart(fig_pais, use_container_width=True)

# 3. Productos más vendidos
df_products_orders = orders_products.merge(products, on="id_producto")
top_productos = df_products_orders.groupby("descripcion")['id_orden'].count().reset_index()
top_productos = top_productos.sort_values("id_orden", ascending=False).head(10)

fig_top = px.bar(
    top_productos, x='id_orden', y='descripcion',
    orientation='h',
    title="🏆 Top 10 Productos Más Órdenes",
    labels={"descripcion": "Producto", "id_orden": "Órdenes"}
)
fig_top.update_layout(yaxis={'categoryorder':'total ascending'})
st.plotly_chart(fig_top, use_container_width=True)
