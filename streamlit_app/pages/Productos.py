import streamlit as st
import pandas as pd
import plotly.express as px

# Ajustes de fuentes 
import plotly.io as pio
from Inicio import kreadores_header , display_metrics
from estilos import load_css

# --------------------------- FUNCIONES
def get_productos_kpis(products):
    # KPIs básicos
    total_productos = products['id_producto'].nunique()
    stock_total = products["stock"].sum()
    productos_bajo_umbral = products[products["stock"] < products["umbral_stock"]].shape[0]


    metrics = [
        ("🎥 Total Productos", f"{total_productos:,}", "#1f77b4"),
        ("🛒 Stock Total", f"{stock_total:,}", "#2ca02c"),
        ("🚨 Bajo Umbral", f"{productos_bajo_umbral:,}", "#ff7f0e")
    ]

    return metrics

def get_kpis(products, orders_products):
    total_productos = products['id_producto'].nunique()
   
  # KPIs adicionales
    productos_sin_stock = products[products["stock"] == 0].shape[0]
    productos_vendidos = orders_products['id_producto'].nunique()
    productos_no_vendidos = total_productos - productos_vendidos

    metrics = [
         ("❌ Sin Stock", f"{productos_sin_stock:,}", "#d62728"),
        ("🔥 Vendidos", f"{productos_vendidos:,}", "#9467bd"),
        ("🧊 Nunca Vendidos", f"{productos_no_vendidos:,}", "#8c564b"),
    ]

    return metrics

def plot_price_distribution(products):
    fig1 = px.histogram(
        products,
        x="precio",
        nbins=30,
        labels={"precio": "Precio", "count": "Cantidad de productos"},
        color_discrete_sequence=["#26A81A"]  # azul profesional
    )
        
    # Ajustes de layout para Streamlit
    fig1.update_layout(
        autosize=True,
        margin=dict(l=40, r=20, t=30, b=50),
        height=400,
        xaxis=dict(tickfont=dict(size=12), title_font=dict(size=14)),
        yaxis=dict(tickfont=dict(size=12), title_font=dict(size=14)),
        bargap=0.1
    )
    st.markdown("### Distribución de Precios de Productos")
    st.plotly_chart(fig1, use_container_width=True)


def plot_top_vendidos(orders_products, products, orders, top_n=10):
    """
    Top N productos más vendidos (basado en órdenes pagadas).
    - Cuenta número de órdenes únicas que incluyen cada producto.
    - Muestra un bar horizontal con cantidad (ordenes) y nombre de producto.
    """
    # Filtrar órdenes pagadas (asegura mayúsc/minúsc)
    ordenes_pagadas = orders[orders['estado_orden'].astype(str).str.lower() == "pagada"]
    if ordenes_pagadas.empty:
        st.warning("⚠️ No hay órdenes pagadas para calcular productos vendidos.")
        return

    # Unir orders_products con las órdenes pagadas
    ventas = orders_products.merge(
        ordenes_pagadas[['id_orden']],
        on='id_orden',
        how='inner'
    )

    if ventas.empty:
        st.warning("⚠️ No hay productos asociados a órdenes pagadas.")
        return

    # Agrupar: contar órdenes únicas por producto (número de órdenes en las que aparece el producto)
    ventas_agg = (
        ventas.groupby('id_producto')['id_orden']
        .nunique()
        .reset_index(name='cantidad')
    )

    # Unir con nombres/descripciones de productos (campo 'descripcion' en tu dataset)
    prod_info = products[['id_producto', 'descripcion']].drop_duplicates()
    ventas_agg = ventas_agg.merge(prod_info, on='id_producto', how='left')
    ventas_agg['descripcion'] = ventas_agg['descripcion'].fillna('Sin descripción')

    # Top N
    topN = ventas_agg.sort_values('cantidad', ascending=False).head(top_n)

    if topN.empty:
        st.warning("⚠️ No se encontraron productos para mostrar.")
        return

    # Gráfica horizontal: cantidad (x) vs descripcion (y)
    fig = px.bar(
        topN,
        x='cantidad',
        y='descripcion',
        orientation='h',
        text='cantidad',
        labels={'cantidad': 'Órdenes que incluyen el producto', 'descripcion': 'Producto'},
        color='cantidad',
        color_continuous_scale='Blues'
    )

    # Mostrar valores con separador de miles y colocar la barra más vendida arriba
    fig.update_traces(texttemplate='%{text:,}', textposition='outside', hovertemplate="<b>%{y}</b><br>Órdenes: %{x:,}<extra></extra>")
    # Layout responsivo y legible
    fig.update_layout(
        yaxis={'categoryorder':'total ascending'},  # mayor arriba
        autosize=True,
        margin=dict(l=200, r=20, t=60, b=40),  # margen izquierdo más grande para nombres largos
        height=500,  # ajustable según cantidad de productos
        plot_bgcolor='white',
        xaxis=dict(tickfont=dict(size=12)),
        yaxis=dict(tickfont=dict(size=12))
    )

    st.markdown(f"### 🏆 Top {top_n} Productos más Vendidos (Órdenes Pagadas)")
    st.plotly_chart(fig, use_container_width=True)

def plot_top_marcas(orders, orders_products, products, top_n=12):
    """
    Top N marcas más populares (basadas en productos de órdenes pagadas).
    """
    # Filtrar órdenes pagadas
    ordenes_pagadas = orders[orders['estado_orden'].astype(str).str.lower() == "pagada"]
    if ordenes_pagadas.empty:
        st.warning("⚠️ No hay órdenes pagadas para calcular marcas populares.")
        return

    # Unir órdenes con productos vendidos
    ventas = orders_products.merge(
        ordenes_pagadas[['id_orden']],
        on='id_orden',
        how='inner'
    )

    if ventas.empty:
        st.warning("⚠️ No hay productos asociados a órdenes pagadas.")
        return

    # Traer marca de cada producto
    ventas = ventas.merge(
        products[['id_producto', 'marca']],
        on='id_producto',
        how='left'
    )

    # Agrupar por marca
    top_marcas = (
        ventas.groupby('marca')['id_orden']
        .nunique()
        .reset_index(name='cantidad')
        .sort_values('cantidad', ascending=False)
        .head(top_n)
    )

    if top_marcas.empty:
        st.warning("⚠️ No se encontraron marcas para mostrar.")
        return

    # Gráfica
    fig = px.bar(
        top_marcas,
        x='marca',
        y='cantidad',
        text='cantidad',
        color='cantidad',
        color_continuous_scale='Blues',
        labels={'marca': 'Marca', 'cantidad': 'Órdenes que incluyen la marca'}
    )

    fig.update_traces(texttemplate='%{text:,}', textposition='outside')
    fig.update_layout(
    autosize=True,
    margin=dict(l=40, r=20, t=60, b=80),
    height=450,  # ajustable según cantidad de marcas
    plot_bgcolor='white',
    xaxis=dict(tickfont=dict(size=12), title_font=dict(size=14)),
    yaxis=dict(tickfont=dict(size=12), title_font=dict(size=14)),
    showlegend=False
    )   

    st.subheader(f"🏆 Top {top_n} Marcas más Compradas")
    st.plotly_chart(fig, use_container_width=True)

def get_productos_extremos(products):
    """
    Retorna el producto más caro y más barato con descripción y precio.
    """
    if products.empty:
        return None, None

    # Producto más caro
    prod_max = products.loc[products['precio'].idxmax(), ['descripcion', 'precio']]
    # Producto más barato
    prod_min = products.loc[products['precio'].idxmin(), ['descripcion', 'precio']]

    return prod_max.to_dict(), prod_min.to_dict()


# -------------------------------- PRODUCTOS ------------- 
load_css()
kreadores_header()

# ---- Configuración de página ----
# st.set_page_config(page_title="Products", layout="wide")
st.markdown('<h2 class="section-title">Products</h2><br>', unsafe_allow_html=True)

# Recuperas los dfs cargados en Home
df_clean = st.session_state.df_clean
products = df_clean['products_clean']
orders = df_clean['orders_clean']
orders_products = df_clean['orders_products_clean']

# ---- KPIs ----
metrics = get_productos_kpis(products)
display_metrics(metrics)

metrics2 = get_kpis(products, orders_products)
display_metrics(metrics2)


# ---- Distribución de precios ----
# st.subheader("Distribución de Precios de Productos")

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
# Ajustes de fuentes (solo una vez por cada eje)
# fig_top_alerta.update_layout(
#     title_font_size=24,              # título
#     xaxis_title_font_size=16,        # eje X título
#     xaxis_tickfont_size=14,          # eje X valores
#     yaxis_title_font_size=16,        # eje Y título
#     yaxis_tickfont_size=14           # eje Y valores
# )

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

