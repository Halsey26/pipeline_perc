import streamlit as st
import pandas as pd
import plotly.express as px

# ---- Configuración de página ----
st.set_page_config(page_title="Clientes", layout="wide")
st.title("👥 Análisis de Clientes")

# ---- Recuperar los DataFrames ----
df_clean = st.session_state.df_clean
customers = df_clean['customers_clean']
orders = df_clean['orders_clean']

# ---- KPIs ----
total_clientes = customers['id_cliente'].nunique()
clientes_con_ordenes = orders['id_cliente'].nunique()
clientes_inactivos = total_clientes - clientes_con_ordenes

col1, col2, col3 = st.columns(3)
col1.metric("Total Clientes", total_clientes)
col2.metric("Clientes con Órdenes", clientes_con_ordenes)
col3.metric("Clientes sin Órdenes", clientes_inactivos)

st.markdown("### 📊 Distribución Geográfica")

# ---- Top 5 ciudades ----
clientes_ciudad = (
    customers.groupby("cliente_ciudad")['id_cliente']
    .count().reset_index()
    .sort_values("id_cliente", ascending=False)
    .head(5)
)
fig_ciudad = px.bar(
    clientes_ciudad, x="cliente_ciudad", y="id_cliente",
    title="Top 5 Ciudades con más Clientes", text_auto=True
)
st.plotly_chart(fig_ciudad, use_container_width=True)

# ---- Top 5 municipalidades ----
clientes_muni = (
    customers.groupby('cliente_municipalidad')['id_cliente']
    .count().reset_index()
    .sort_values('id_cliente', ascending=False)
    .head(5)
)
fig_muni = px.bar(
    clientes_muni, x='cliente_municipalidad', y='id_cliente',
    title="Top 5 Municipalidades con más Clientes", text_auto=True
)
st.plotly_chart(fig_muni, use_container_width=True)

st.markdown("### 📊 Segmentación de Clientes")

# ---- Marketing ----
clientes_marketing = customers['acepta_marketing'].value_counts().reset_index()
clientes_marketing.columns = ['acepta_marketing', 'conteo']
fig_marketing = px.pie(
    clientes_marketing, names='acepta_marketing', values='conteo',
    title="Clientes que Aceptan Marketing"
)
st.plotly_chart(fig_marketing, use_container_width=True)

# ---- Status ----
clientes_status = customers['status'].value_counts().reset_index()
clientes_status.columns = ['status', 'conteo']
fig_status = px.pie(
    clientes_status, names='status', values='conteo',
    title="Clientes por Estado"
)
st.plotly_chart(fig_status, use_container_width=True)

st.markdown("### 🏆 Top Clientes")

# ---- Top clientes por gasto ----
clientes_gasto = (
    orders.groupby('id_cliente')['precio_total'].sum().reset_index()
    .merge(customers[['id_cliente','cliente_nombre']], on='id_cliente', how='left')
    .sort_values('precio_total', ascending=False)
    .head(10)
)
fig_top = px.bar(
    clientes_gasto, x='cliente_nombre', y='precio_total',
    title="Top 10 Clientes por Gasto", text_auto=True
)
fig_top.update_xaxes(tickangle=45)
st.plotly_chart(fig_top, use_container_width=True)
