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
from funciones_pag.Inicio import *
from funciones_pag.Clientes import *
from funciones_pag.Ordenes import *
from funciones_pag.Productos import *
import joblib
#------ FUNCIONES DE CADA PÁGINA

def pag_Inicio(data):
    # ---- DATA ----
    df_clean = data
    # ---- Página ----
    kreadores_header()
    pie_pagina("https://ecommerce-ia-backend-nyg4.onrender.com/")

    customers = df_clean['customers_clean']
    orders = df_clean['orders_clean']
    products = df_clean['products_clean']
    orders_products = df_clean['orders_products_clean']

    # ---- KPIs ----
    metrics = get_kpis_inicio(customers, orders)
    display_metrics(metrics)

    # ---- VISUALIZACIONES ----
    st.markdown("*Insights accionables específicos para tu tienda de equipos fotográficos y de video*")

    st.markdown('<h2 class="section-title">📊 Análisis General</h2>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    # 1. Ventas en el tiempo        
    with col1:
        st.markdown("###  Ventas Mensuales")
        ventas_mensuales(orders)

    with col2:
        st.markdown("### Resumen de Ventas 2025")
        resumen_df = resumen_ventas(orders)
        st.table(resumen_df)

        st.markdown(f"""
        <div class="priority-low">
        <strong>Nota:</strong><br>
        Esta información ha sido filtrada solo para las órdenes pagadas. (Ventas sí realizadas)
        </div>
        """, unsafe_allow_html=True)


    # 2. Por geografia 
    col1, col2 = st.columns(2)
    with col1:
        nube_municipalidades_order(orders)
    with col2:
        nube_municipalidades_clientes(customers)

    # 3. Por Productos
    st.markdown("### Top 10 Productos Más Populares")
    top_productos_populares(orders_products, products)

    st.markdown("### Top 10 Productos Más Vendidos")
    top_productos_vendidos(orders, orders_products, products)

    # --------- RECOMENDACIONES --------------
    st.markdown('<h2 class="section-title">📝 Recomendaciones y Observaciones</h2>', unsafe_allow_html=True)
    # st.markdown(recomendaciones_dashboard(orders, ustomers, products, orders_products))
    tendencia,producto_top,municipio_top,ticket_promedio,reco_ticket,tasa_recompra,reco_recompra,ingresos_totales,nro_ordenes = recomendaciones_dashboard(orders, customers, products, orders_products)
    st.markdown(f"""
    <div class="priority-high">

    - <strong>Tendencia de ventas:</strong> Actualmente {tendencia}. <br>

    - <strong>Producto destacado :</strong> El más vendido es {producto_top}. <br>
    - <strong>Concentración geográfica :</strong> La mayoría de las ventas provienen de {municipio_top}. <br>
    - <strong>Ticket promedio :</strong> {ticket_promedio:,.0f} CLP por orden → {reco_ticket} <br>
    - <strong>Propensión de recompra :</strong> {tasa_recompra:.2f}% de clientes repiten compra → {reco_recompra} <br>
    - <strong>Ingresos acumulados:</strong> {ingresos_totales:,.0f} CLP en {nro_ordenes} ventas.

    </div>
      """, unsafe_allow_html=True)

    # ---------- DESCRIPCIONES
    # explicar la definicion de tickect promedio, tasa de recompra  el aov
    st.markdown('<h2 class="section-title">📖 Definiciones de métricass</h2>', unsafe_allow_html=True)
    st.markdown(f"""
    <div class="priority-low">

    - <strong>Ticket Promedio:</strong> Valor promedio de compra por cliente en cada orden.  <br>
    Se calcula como los ingresos totales divididos entre el número de órdenes pagadas.  <br>
    Indica cuánto gasta, en promedio, un cliente por pedido.  

    - <strong>Tasa de Recompra:</strong> Porcentaje de clientes que realizaron más de una compra en el periodo analizado.  <br>
    Refleja la fidelidad de los clientes y la efectividad de las estrategias de retención.  

    - <strong>AOV (Average Order Value):</strong> Métrica muy relacionada con el ticket promedio.  <br>
    Representa el ingreso promedio generado por cada orden efectivamente pagada.  <br>
    Una subida en el AOV suele asociarse con estrategias de <i>upselling</i> y <i>cross-selling</i>.  

    </div>
    """, unsafe_allow_html=True)

def pag_Clientes(data):
    # ---- DATA ----
    df_clean = data
    customers = df_clean['customers_clean']
    orders = df_clean['orders_clean']

    # ---- Página ----
    kreadores_header()
    pie_pagina("https://ecommerce-ia-backend-nyg4.onrender.com/")

    # ---- Configuración de página ----
    # st.set_page_config(page_title="Clientes", layout="wide")
    st.markdown('<h2 class="section-title">Clientes</h2><br>', unsafe_allow_html=True)


    # ---- KPIs ----
    metrics = get_clientes_kpis(customers, orders)
    display_metrics(metrics)

    metrics_por = kpis_por(customers, orders)
    display_metrics(metrics_por)

    # ---- Top 5 ciudades ----
    st.markdown('<h3 class="section-title">📊 Distribución Geográfica</h3>', unsafe_allow_html=True)

    st.markdown("### Top 5 Ciudades con más Clientes")
    geo_clientes(customers,0)
    st.markdown("###  Top 5 Municipalidades con más Clientes")
    geo_clientes(customers,1)


    # st.markdown("### 📊 Segmentación de Clientes")
    st.markdown('<h3 class="section-title">📊 Segmentación de Cliente</h3>', unsafe_allow_html=True)
    # ---- Marketing ----
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Clientes que Aceptan Marketing")
        marketing_distribucion(customers)
    with col2:
        st.markdown("### Conversión de Clientes según Marketing")
        marketing_conversion(customers, orders)

    st.markdown('<h3 class="section-title">🏆 Top Clientes</h3>', unsafe_allow_html=True)
    gasto_frec(orders, customers)

def pag_ordenes(data):
    # ---- DATA ----
    df_clean = data
    customers = df_clean['customers_clean']
    orders = df_clean['orders_clean']

    # ---- Página ----
    kreadores_header()
    pie_pagina("https://ecommerce-ia-backend-nyg4.onrender.com/")

    # ---- Configuración de página ----

    st.markdown('<h2 class="section-title"> Órdenes</h2><br>', unsafe_allow_html=True)

    # ---- KPIs ----
    metrics = kpis_ordenes( orders)
    display_metrics(metrics)

    metrics_por = kpis_porc( orders)
    display_metrics(metrics_por)


    # ---- Órdenes en el tiempo ----
    grafica_tiempo(orders)

    col1, col2 = st.columns(2)

    with col1:
        # ---- Estado de órdenes ----
        plot_estado_orden(orders)    
        
    with col2:
        # ---- Estado cumplimiento/envío ----
        plot_cumplimiento(orders)


    # ---- Empresa de envío ----
    distri_empresa(orders)

    # ---- Top 10 regiones ----
    top_regiones(orders)

def pag_productos(data):
    # ---- DATA ----
    df_clean = data
    products = df_clean['products_clean']
    orders = df_clean['orders_clean']
    orders_products = df_clean['orders_products_clean']

    # ---- Página ----
    kreadores_header()
    pie_pagina("https://ecommerce-ia-backend-nyg4.onrender.com/")

    # ---- Configuración de página ----
    st.set_page_config(page_title="Products", layout="wide")
    st.markdown('<h2 class="section-title">Products</h2><br>', unsafe_allow_html=True)

    # ---- KPIs ----
    metrics = get_productos_kpis(products)
    display_metrics(metrics)

    metrics2 = get_kpis(products, orders_products)
    display_metrics(metrics2)

    # ---- Distribución de precios ----
    st.subheader("Distribución de Precios de Productos")

    col1, col2 = st.columns(2)
    with col1:
        plot_price_distribution(products)
    with col2:
        st.subheader(" ")
        prod_max, prod_min = get_productos_extremos(products)
        st.markdown(f"""
            <div class="priority-low">

    - <strong>Producto más caro:</strong> <br>
            💎 {prod_max['descripcion']} - Precio: ${prod_max['precio']:,}
    - <strong>Producto más económico:</strong> <br>        
            ⚡ {prod_min['descripcion']} - Precio: ${prod_min['precio']:,}
            </div>
            """, unsafe_allow_html=True)


    # Productos más vendidos
    plot_top_vendidos(orders_products, products, orders)

    # ---- Productos bajo umbral de stock ----
    productos_alerta = products[products['stock'] <= products['umbral_stock']].copy()
    productos_alerta['faltante'] = productos_alerta['umbral_stock'] - productos_alerta['stock']

    # Seleccionamos columnas relevantes
    tabla_alerta = productos_alerta[['descripcion', 'stock', 'umbral_stock', 'faltante']].sort_values('faltante', ascending= False)

    st.subheader("🚨 Productos en Alerta de Reposición")
    st.dataframe(tabla_alerta, use_container_width=True)

    #---- Top 10 productos con mayor faltante ----
    st.subheader("Top 10 Productos más Críticos (faltante)")
    top_alerta = tabla_alerta.sort_values("faltante", ascending=False).head(10)
    fig_top_alerta = px.bar(
        top_alerta,
        x="faltante", y="descripcion",  # Cambiamos orden
        orientation="h",             # Horizontal
        title=" ",
        labels={"descripcion": "Producto", "faltante": "Unidades a reponer"},
        text_auto=True
    )

    # Layout responsivo y legible
    fig_top_alerta.update_layout(
        autosize=True,
        margin=dict(l=200, r=20, t=40, b=40),  # margen izquierdo grande para nombres largos
        height=500,
        xaxis=dict(tickfont=dict(size=12), title_font=dict(size=14)),
        yaxis=dict(tickfont=dict(size=12), title_font=dict(size=14))
    )

    # Aumentar tamaño del texto en las barras
    fig_top_alerta.update_traces(textfont_size=20)

    st.plotly_chart(fig_top_alerta, use_container_width=True)

    # ---- Distribución por marca ----
    plot_top_marcas(orders, orders_products, products)

def churn(data):
    # ---- DATA ----
    df_clean = data
    products = df_clean['products_clean']
    orders = df_clean['orders_clean']
    orders_products = df_clean['orders_products_clean']

    # ---- Página ----
    kreadores_header()
    pie_pagina("https://ecommerce-ia-backend-nyg4.onrender.com/")
  
    # modelo
    BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
    parent_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))
    ruta= os.path.join(parent_dir,"streamlit_app",'modelo', "modelo_churn.pkl")
    pipe = joblib.load(ruta) #retroceder una carpeta y ruta
    # features = joblib.load("features.pkl")

    st.header("Predicción de Churn")

    recency_days = st.number_input("Días desde la última compra", min_value=0, max_value=500, value=100)
    frequency_all = st.number_input("Total de compras (histórico)", min_value=1, value=3)
    freq_30 = st.number_input("Compras últimos 30 días", min_value=0, value=1)
    freq_90 = st.number_input("Compras últimos 90 días", min_value=0, value=2)
    monetary = st.number_input("Monto total gastado", min_value=0.0, value=300000.0)

    if st.button("Predecir"):
        X_new = [[recency_days, frequency_all, freq_30, freq_90, monetary]]
        pred = pipe.predict(X_new)[0]
        prob = pipe.predict_proba(X_new)[0,1]
        
        if pred == 1:
            st.error(f"⚠️ Cliente propenso a churn (prob: {prob:.2f})")
        else:
            st.success(f"✅ Cliente activo (prob churn: {prob:.2f})")

     

#--------------------- 
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

logo_lateral()

# Navegación con colores de Kreadores
page = st.sidebar.radio(
    "🎯 SELECCIONA:",
    ("Inicio", "Clientes", "Órdenes", "Productos", 'Churn'),
    index=0
)


if page=='Inicio':
    pag_Inicio(data= df_clean)
elif page=='Clientes':
    pag_Clientes(data=df_clean)
elif page=='Órdenes':
    pag_ordenes(data=df_clean)
elif page=='Productos':
    pag_productos(data=df_clean)
elif page== 'Churn':
    churn(data=df_clean)
else:
    st.markdown("👁️👄👁️")
