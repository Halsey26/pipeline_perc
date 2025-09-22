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
from estilos import load_css


# Ejecutar: python -m pipeline_perc.streamlit_app.dashboard
# Para streamlit: streamlit run pipeline_perc/streamlit_app/Inicio.py


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
# st.set_page_config(page_title="Kreadores PRO Dashboard", layout="wide")
# st.title("📊 Kreadores PRO - Dashboard")
# st.sidebar.header("Filtros")


# --- Configuración de la página ---
st.set_page_config(
    page_title="📊 Kreadores PRO Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="📷"
)

load_css()


# --- Header de Kreadores ---
def kreadores_header():
    st.markdown("""
    <div class="main-header">
        <div class="camera-icon">📷</div>
        <div class="kreadores-logo">KREADORES PRO</div>
        <div class="tagline"> Analytics Dashboard – Clientes, Órdenes y Productos Reales del E-commerce</div>
        <div class="subtagline">Análisis de registros, comportamiento de compra y desempeño de productos en Kreadores</div>
    </div>
    """, unsafe_allow_html=True)

# --- Función para mostrar métricas ---
def display_metrics(metrics):
    cols = st.columns(len(metrics))
    for i, (label, value, color) in enumerate(metrics):
        with cols[i]:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">{label}</div>
                <div class="metric-value" style="color: {color};">{value}</div>
            </div>
            """, unsafe_allow_html=True)

# --- Función para calcular y devolver métricas ---
def get_kpis(customers, orders):
    # Total clientes
    total_clientes = customers['id_cliente'].nunique()
    
    # Total órdenes
    total_ordenes = orders['id_orden'].nunique()
    
    # Ingresos totales
    df_orders = orders[orders['estado_orden'] == "Pagada"]

    ingresos_totales = df_orders['precio_total'].sum()

    # Nro de ventas totales
    nro_ventas_totales=df_orders['id_orden'].nunique()

    # aov
    aov = ingresos_totales / nro_ventas_totales if nro_ventas_totales > 0 else 0
    
    # Ticket promedio (ingreso por orden)
    ticket_promedio = ingresos_totales / total_ordenes if total_ordenes > 0 else 0
    
    # Gasto promedio por cliente
    gasto_promedio_cliente = ingresos_totales / total_clientes if total_clientes > 0 else 0
    
    # Clientes con más de 1 compra
    compras_por_cliente = df_orders.groupby("id_cliente")['id_orden'].count()
    clientes_recompra = (compras_por_cliente > 1).sum()
    tasa_recompra = clientes_recompra / total_clientes * 100 if total_clientes > 0 else 0
    

    # Crear lista de métricas con label, valor y color
    metrics = [
        # ("👥 Clientes", f"{total_clientes:,}", "#1f77b4"),
        # ("🛒 Órdenes", f"{total_ordenes:,}", "#ff7f0e"),
        ("💰 Ingresos Totales (CLP)", f"${ingresos_totales:,.2f}", "#2ca02c"),
        ("🎟️ Ticket Promedio", f"${ticket_promedio:,.2f}", "#d62728"),
        ("📈 Gasto Promedio por Cliente", f"${gasto_promedio_cliente:,.0f}", "#9467bd"),
        ("💱 Propensión a Recompra", f"{tasa_recompra:,.2f}%", "#8c564b"),
        ("💸 Número de Ventas", f"{nro_ventas_totales}", "#3ba2a4"),
        ("💲 AOV", f"{aov:,.0f}", "#A8E354"),
    ]
    # agregar la cantidad de ventas
    return metrics


# 1. Ventas en el tiempo
def ventas_mensuales(orders):
    df_orders = orders.copy()
    df_orders['fecha_creacion'] = pd.to_datetime(df_orders['fecha_creacion'])
    
    # ✅ Filtrar solo órdenes pagadas
    df_orders = df_orders[df_orders['estado_orden'] == "Pagada"]

    ventas_tiempo = (df_orders.groupby(df_orders['fecha_creacion'].dt.to_period("M"))['precio_total'].sum().reset_index())
    ventas_tiempo['fecha_creacion'] = ventas_tiempo['fecha_creacion'].astype(str)

    fig_time = px.line(
        ventas_tiempo, x='fecha_creacion', y='precio_total',
        markers=True,
        labels={"fecha_creacion": "Fecha", "precio_total": "Ingresos (CLP)"}
    )
    st.plotly_chart(fig_time, use_container_width=True)


def resumen_ventas(orders):
    df_orders = orders.copy()
    df_orders['fecha_creacion'] = pd.to_datetime(df_orders['fecha_creacion'])
    
    # Filtrar solo ventas reales
    df_orders = df_orders[df_orders['estado_orden'] == "Pagada"]
    
    # Agrupar por mes
    ventas_tiempo = (
        df_orders
        .groupby(df_orders['fecha_creacion'].dt.to_period("M"))
        .agg({"precio_total": "sum", "id_orden": "count"})
        .reset_index()
    )
    ventas_tiempo.rename(columns={"precio_total": "ingresos", "id_orden": "num_ordenes"}, inplace=True)

    # ventas_tiempo['fecha_creacion'] = ventas_tiempo['fecha_creacion'].astype(str)
    ventas_tiempo['mes_texto'] = ventas_tiempo['fecha_creacion'].dt.to_timestamp().dt.strftime("%B")


    # Filtrar solo 2025
    ventas_2025 = ventas_tiempo[ventas_tiempo['fecha_creacion'].astype(str).str.startswith("2025")]
    

    if ventas_2025.empty:
        return "No hay ventas registradas en 2025."

    # Año y mes actual
    hoy = datetime.today()
    año_actual = hoy.year
    mes_actual = hoy.strftime("%B")

    # 📈 Mes con más ventas
    top_mes = ventas_2025.loc[ventas_2025['ingresos'].idxmax()]

    # 🔻 Mes con menos ventas
    low_mes = ventas_2025.loc[ventas_2025['ingresos'].idxmin()]

    # 📅 Ventas acumuladas del año
    total_2025 = ventas_2025['ingresos'].sum()
    ordenes_2025 = ventas_2025['num_ordenes'].sum()

    # 📊 Ventas en curso (mes actual)
    ventas_mes_actual = ventas_2025[ventas_2025['mes_texto'] == mes_actual]
    # Ordenes en curso (mes actual)
    ordenes_mes_actual = ventas_2025[ventas_2025['mes_texto'] == mes_actual]['num_ordenes'].sum()

    # 🚀 Comparación con mes anterior
    try:
        idx_actual = ventas_2025.index[ventas_2025['mes_texto'] == mes_actual][0]
        if idx_actual > 0:
            ingresos_actual = ventas_2025.loc[idx_actual, "ingresos"]
            ingresos_anterior = ventas_2025.loc[idx_actual - 1, "ingresos"]
            tendencia = "⬆️ Mayor que el mes anterior" if ingresos_actual > ingresos_anterior else "⬇️ Menor que el mes anterior"
        else:
            tendencia = "Sin datos del mes anterior"
    except:
        tendencia = "No disponible"

    # Generar resumen tipo ejecutivo
    resumen_data = [
        ["Mes con mayores ventas", top_mes['mes_texto'], f"{top_mes['ingresos']:,} CLP", f"{top_mes['num_ordenes']}"],
        ["Mes con menores ventas", low_mes['mes_texto'], f"{low_mes['ingresos']:,} CLP", f"{low_mes['num_ordenes']}"],
        ["Ventas acumuladas en 2025", f"January-{mes_actual}", f"{total_2025:,} CLP", f"{ordenes_2025}"],
        ["Ventas en curso", mes_actual, f"{ventas_mes_actual['ingresos'].values[0] if not ventas_mes_actual.empty else 0:,} CLP", ordenes_mes_actual],
        ["Tendencia", "", tendencia, ""]
    ]

    # Convertir a DataFrame
    resumen_df = pd.DataFrame(resumen_data, columns=["Métrica", "Periodo", "Resultado", "Nro Ventas"])

    return resumen_df

def nube_municipalidades_order(orders):
    """
    Genera una nube de palabras con las municipalidades
    donde se realizaron más ventas (solo órdenes cumplidas).
    """
    # Filtrar solo órdenes pagadas
    ventas = orders[orders['estado_orden'] == "Pagada"]

    if ventas.empty:
        st.warning("⚠️ No hay órdenes cumplidas para generar la nube de municipalidades.")
        return

    # Contar municipalidades con más ventas
    municipios_count = ventas['municipalidad_envio'].value_counts()

    if municipios_count.empty:
        st.warning("⚠️ No se encontraron municipalidades en las órdenes cumplidas.")
        return

    # Convertir a diccionario para WordCloud
    municipios_dict = municipios_count.to_dict()

    # Crear la nube de palabras
    wordcloud = WordCloud(
        width=800,
        height=600,
        background_color="white",
        colormap="viridis"  # opciones: 'plasma', 'cool', 'inferno', etc.
    ).generate_from_frequencies(municipios_dict)

    # Mostrar en Streamlit
    st.markdown("### 🌎 Municipalidades con más Ventas")
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.imshow(wordcloud, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)

    # Nota explicativa
    st.caption("🔎 Mientras más grande aparece el nombre, más ventas hubo en esa municipalidad.")

# clientes_municipalidades
def nube_municipalidades_clientes(customer):
    """
    Genera una nube de palabras con las municipalidades
    donde se realizaron más ventas (solo órdenes cumplidas).
    """
    
    clientes= customer.copy()

    # Limpiar valores nulos o 'Sin especificar'
    clientes = clientes[
        clientes['cliente_municipalidad']
        .fillna("")
        .str.strip()
        .str.lower() != "sin especificar"
    ]

    if clientes.empty:
        st.warning("⚠️ No hay clientes registrados para generar la nube de municipalidades.")
        return

    # Contar municipalidades con más ventas
    municipios_count = clientes['cliente_municipalidad'].value_counts()

    if municipios_count.empty:
        st.warning("⚠️ No se encontraron municipalidades.")
        return

    # Convertir a diccionario para WordCloud
    municipios_dict = municipios_count.to_dict()

    # Crear la nube de palabras
    wordcloud = WordCloud(
        width=800,
        height=600,
        background_color="white",
        colormap="viridis"  # opciones: 'plasma', 'cool', 'inferno', etc.
    ).generate_from_frequencies(municipios_dict)

    # Mostrar en Streamlit
    st.markdown("### 👤 Municipalidades con más Usuarios")
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.imshow(wordcloud, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)

    # Nota explicativa
    st.caption("🔎 Mientras más grande aparece el nombre, más clientes hay en esa municipalidad.")

def top_productos_populares(orders_products, products):
    """
    Genera el top 10 de productos más populares (por número de órdenes).
    """
    df_products_orders = orders_products.merge(products, on="id_producto")
    top_productos = df_products_orders.groupby("descripcion")['id_orden'].count().reset_index()
    top_productos = top_productos.sort_values("id_orden", ascending=False).head(10)

    fig_top = px.bar(
        top_productos, 
        x='id_orden', 
        y='descripcion',
        orientation='h',
        labels={"descripcion": "Producto", "id_orden": "Órdenes"},
        color='id_orden',                 # columna numérica para asignar color
        color_continuous_scale='inferno'   # aquí sí entra la paleta
    )
    # fig_top.update_layout(yaxis={'categoryorder':'total ascending'})
    fig_top.update_layout(
    yaxis=dict(
        categoryorder='total ascending',
        tickfont=dict(size=14) ), 
        margin=dict(l=250)  )

    st.plotly_chart(fig_top, use_container_width=True)

def top_productos_vendidos(orders, orders_products, products):
    """
    Genera el top 10 de productos más vendidos (solo órdenes pagadas).
    """
    # Filtrar órdenes pagadas
    ordenes_vendidas = orders[orders['estado_orden'] == "Pagada"]

    if ordenes_vendidas.empty:
        st.warning("⚠️ No hay órdenes pagadas para mostrar el top de productos.")
        return

    # Merge con orders_products y products
    df_products_orders = (
        orders_products
        .merge(ordenes_vendidas[['id_orden']], on="id_orden")   # solo las órdenes pagadas
        .merge(products, on="id_producto")
    )

    if df_products_orders.empty:
        st.warning("⚠️ No hay productos asociados a órdenes pagadas.")
        return

    # Contar productos vendidos
    top_productos = (
        df_products_orders.groupby("descripcion")['id_orden']
        .count()
        .reset_index(name="num_ordenes")
        .sort_values("num_ordenes", ascending=False)
        .head(10)
    )

    # Gráfica
    fig_top = px.bar(
        top_productos,
        x='num_ordenes',
        y='descripcion',
        orientation='h',
        labels={"descripcion": "Producto", "num_ordenes": "Ventas"},
        color='num_ordenes',                 
        color_continuous_scale='inferno'   
    )


    fig_top.update_layout(
    yaxis=dict(
        categoryorder='total ascending',
        tickfont=dict(size=14)
    ),
    margin=dict(l=250)  #  aumenta el margen izquierdo para que entren textos largos
)

    st.plotly_chart(fig_top, use_container_width=True)

def recomendaciones_dashboard(orders, customers, products, orders_products):
    """
    Genera un bloque dinámico de recomendaciones basado en los datos de ventas y clientes.
    """

    # --- Filtrar órdenes pagadas (ventas reales) ---
    ventas = orders[orders['estado_orden'] == "Pagada"].copy()
    if ventas.empty:
        return "⚠️ No hay ventas registradas para generar recomendaciones."

    # --- Tendencia de ventas ---
    ventas['fecha_creacion'] = pd.to_datetime(ventas['fecha_creacion'])
    ventas['mes'] = ventas['fecha_creacion'].dt.to_period("M")
    ventas_mensuales = ventas.groupby('mes')['precio_total'].sum().reset_index()

    if len(ventas_mensuales) >= 2:
        ult_mes = ventas_mensuales.iloc[-1]['precio_total']
        ant_mes = ventas_mensuales.iloc[-2]['precio_total']
        if ult_mes > ant_mes:
            tendencia = "📈 al alza"
        elif ult_mes < ant_mes:
            tendencia = "📉 a la baja"
        else:
            tendencia = "➡️ estable"
    else:
        tendencia = "ℹ️ Sin datos suficientes para calcular tendencia"

    # --- Producto más vendido ---
    ventas_productos = (
        orders_products
        .merge(ventas[['id_orden']], on="id_orden")
        .merge(products[['id_producto', 'descripcion']], on="id_producto")
    )
    producto_top = (
        ventas_productos['descripcion']
        .value_counts()
        .idxmax()
        if not ventas_productos.empty else "N/A"
    )

    # --- Municipalidad con más ventas ---
    municipio_top = (
        ventas['municipalidad_envio']
        .value_counts()
        .idxmax()
        if not ventas['municipalidad_envio'].empty else "N/A"
    )

    # --- KPIs adicionales ---
    ingresos_totales = ventas['precio_total'].sum()
    nro_ordenes = ventas['id_orden'].nunique()
    total_clientes = customers['id_cliente'].nunique()
    ticket_promedio = ingresos_totales / nro_ordenes if nro_ordenes > 0 else 0
    compras_por_cliente = ventas.groupby("id_cliente")['id_orden'].count()
    clientes_recompra = (compras_por_cliente > 1).sum()
    tasa_recompra = clientes_recompra / total_clientes * 100 if total_clientes > 0 else 0

    # Lógica de recomendaciones
    if ticket_promedio < 20000:
        reco_ticket = "el ticket promedio es bajo; considera bundles, upselling en el checkout y promociones por monto mínimo."
    elif ticket_promedio < 60000:
        reco_ticket = "el ticket promedio es moderado; puedes optimizar cross-selling y promociones segmentadas para elevar el valor por compra."
    else:
        reco_ticket = "el ticket promedio es alto; enfócate en retener clientes premium con beneficios exclusivos y experiencias VIP."

    if tasa_recompra < 10:
        reco_recompra = "la recompra es baja; sería clave implementar campañas de retención, recordatorios post-compra y programas de fidelización."
    elif tasa_recompra < 25:
        reco_recompra = "la recompra es moderada; aún hay espacio para mejorar con incentivos personalizados y suscripciones."
    else:
        reco_recompra = "la recompra es alta; aprovecha para **escalar con programas VIP y referidos**."


    # --- Bloque dinámico de recomendaciones ---
    recomendaciones = f"""
    - **Tendencia de ventas**: Actualmente {tendencia}.
    - **Producto destacado**: El más vendido es {producto_top}.
    - **Concentración geográfica**: La mayoría de las ventas provienen de {municipio_top}.
    - **Ticket promedio**: {ticket_promedio:,.0f} CLP por orden → {reco_ticket}
    - **Propensión de recompra**: {tasa_recompra:.2f}% de clientes repiten compra → {reco_recompra}
    - **Ingresos acumulados**: {ingresos_totales:,.0f} CLP en {nro_ordenes} ventas.
    """
    # recomendaciones = recomendaciones.replace("\n", "<br>")

    return tendencia,producto_top,municipio_top,ticket_promedio,reco_ticket,tasa_recompra,reco_recompra,ingresos_totales,nro_ordenes


# --------------------------------INICIO APP--------------------------------------------------- 
kreadores_header()

# ---- DATA ----
df_clean = st.session_state.df_clean

customers = df_clean['customers_clean']
orders = df_clean['orders_clean']
products = df_clean['products_clean']
orders_products = df_clean['orders_products_clean']

# ---- KPIs ----
metrics = get_kpis(customers, orders)
display_metrics(metrics)

# ---- VISUALIZACIONES ----
# st.markdown("*Insights accionables específicos para tu tienda de equipos fotográficos y de video*")

st.markdown('<h2 class="section-title">📊 Análisis General</h2>', unsafe_allow_html=True)
col1, col2 = st.columns(2)

# 1. Ventas en el tiempo        
with col1:
    st.markdown("###  Ventas Mensuales")
    # st.markdown("*Insights accionables específicos para tu tienda de equipos fotográficos y de video*")
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
# st.markdown(recomendaciones_dashboard(orders, customers, products, orders_products))
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

# --- Pie de página ---
st.sidebar.markdown("---")
st.sidebar.info("""
**Kreadores Analytics Dashboard**  
v2.1 · Actualizado: {date}  
Inspírate, crea y lleva tus ideas al siguiente nivel.
Powered by Supabase
""".format(date=pd.Timestamp.now().strftime("%Y-%m-%d")))

st.sidebar.markdown("""
<div style="text-align: center; margin-top: 20px;">
    <a href="https://www.kreadores.pro" target="_blank" style="color: #667eea; text-decoration: none;">
        🌐 www.kreadores.pro
    </a>
</div>
""", unsafe_allow_html=True)