import streamlit as st
import pandas as pd
import plotly.express as px

# Recuperas los dfs cargados en Home
df_clean = st.session_state.df_clean

customers = df_clean['customers_clean']
orders = df_clean['orders_clean']

st.set_page_config(page_title="Órdenes", layout="wide")
st.title("👥 Análisis de Órdenes")

# ---- KPIs ----
col1, col2, col3 = st.columns(3)
col1.metric("Total Órdenes", f"{orders['id_orden'].nunique():,}")
col2.metric("Ingresos Totales", f"${orders['precio_total'].sum():,.2f}")
col3.metric("Ticket Promedio", f"${orders['precio_total'].mean():,.2f}")

# ---- Órdenes en el tiempo ----
orders['fecha_creacion'] = pd.to_datetime(orders['fecha_creacion'])
ordenes_time = orders.groupby(orders['fecha_creacion'].dt.to_period("M")).agg({
    "id_orden": "nunique",
    "precio_total": "sum"
}).reset_index()
ordenes_time['fecha_creacion'] = ordenes_time['fecha_creacion'].astype(str)

fig1 = px.line(ordenes_time, x="fecha_creacion", y=["id_orden","precio_total"],
               labels={"value": "Cantidad / Ingresos", "fecha_creacion": "Fecha"},
               title="Órdenes e Ingresos en el Tiempo")
st.plotly_chart(fig1, use_container_width=True)

# ---- Estado de órdenes ----
fig2 = px.pie(orders, names="estado_orden", title="Distribución de Estado de Órdenes")
st.plotly_chart(fig2, use_container_width=True)

# ---- Estado cumplimiento/envío ----
cumplimiento = orders.groupby("estado_cumplimiento")["id_orden"].count().reset_index()
fig3 = px.bar(cumplimiento, x="estado_cumplimiento", y="id_orden",
              title="Estado de Cumplimiento")
st.plotly_chart(fig3, use_container_width=True)

# ---- Empresa de envío ----
empresa = orders.groupby("empresa_envio")["id_orden"].count().reset_index()
fig4 = px.bar(empresa, x="empresa_envio", y="id_orden",
              title="Órdenes por Empresa de Envío")
st.plotly_chart(fig4, use_container_width=True)

# ---- Top 10 regiones ----
top_regiones = orders.groupby("region_envio")["id_orden"].count().reset_index().nlargest(10, "id_orden")
fig5 = px.bar(top_regiones, x="region_envio", y="id_orden",
              title="Top 10 Regiones con más Órdenes")
st.plotly_chart(fig5, use_container_width=True)