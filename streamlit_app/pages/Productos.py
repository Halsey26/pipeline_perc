import streamlit as st
import pandas as pd
import plotly.express as px

# Ajustes de fuentes 
import plotly.io as pio

# ---- Configuración global ----
# pio.templates["custom"] = pio.templates["plotly"]  # hereda de "plotly" clásico
# pio.templates["custom"].layout.font.size = 16      # tamaño general de fuente
# pio.templates["custom"].layout.title.font.size = 24
# pio.templates["custom"].layout.xaxis.title.font.size = 18
# pio.templates["custom"].layout.yaxis.title.font.size = 18
# pio.templates["custom"].layout.xaxis.tickfont.size = 14
# pio.templates["custom"].layout.yaxis.tickfont.size = 14

# Activar como plantilla por defecto
# pio.templates.default = "custom"

# Recuperas los dfs cargados en Home
df_clean = st.session_state.df_clean

products = df_clean['products_clean']

st.set_page_config(page_title="Products", layout="wide")
st.title("👥 Análisis de Products")

# ---- KPIs ----
total_productos = len(products)
stock_total = products["stock"].sum()
productos_bajo_umbral = products[products["stock"] < products["umbral_stock"]].shape[0]


col1, col2, col3 = st.columns(3)
col1.metric("🎥 Total Productos", f"{total_productos}")
col2.metric("🛒 Stock Total", f"{stock_total}")
col3.metric("🚨 Alerta Stock", f"{productos_bajo_umbral}")

# ---- Distribución de precios ----
st.subheader("Distribución de Precios de Productos")
fig1 = px.histogram(products, x="precio", nbins=30,
                    # title="Distribución de Precios de Productos",
                    labels={"precio": "Precio"})
st.plotly_chart(fig1, use_container_width=True)

# ---- Top productos por stock ----

st.subheader("🌳 Distribución de Stock por marca- Top 10 Productos")
top_stock = products.sort_values("stock", ascending=False).head(10)
fig_treemap = px.treemap(
    top_stock,
    path=["marca","descripcion"],   # Cada rectángulo es un producto
    values="stock",         # Tamaño proporcional al stock
    # title="🌳 Distribución de Stock por marca- Top 10 Productos",
)
fig_treemap.update_traces(textfont_size=40)

st.plotly_chart(fig_treemap, use_container_width=True)


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
fig_top_alerta.update_layout(
    title_font_size=24,              # título
    xaxis_title_font_size=16,        # eje X título
    xaxis_tickfont_size=14,          # eje X valores
    yaxis_title_font_size=16,        # eje Y título
    yaxis_tickfont_size=14           # eje Y valores
)

# Aumentar tamaño del texto en las barras
fig_top_alerta.update_traces(textfont_size=20)

st.plotly_chart(fig_top_alerta, use_container_width=True)



# ---- Distribución por marca ----
st.subheader("Top 12 - Marcas más populares")

top_marcas = products["marca"].value_counts().reset_index(name="count")
top_marcas.columns = ["marca", "count"]
top_marcas= top_marcas.head(12)

fig4 = px.bar(
    top_marcas,
    x="marca", y="count",
    # title="Top 12 Marca",
    text_auto=True
)
st.plotly_chart(fig4, use_container_width=True)

