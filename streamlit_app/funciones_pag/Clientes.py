import streamlit as st
import pandas as pd
import plotly.express as px
from funciones_pag.Inicio import kreadores_header , display_metrics
from estilos import load_css, colores


# ------------- FUNCIONES

def get_clientes_kpis(customers, orders):
    # Filtrar solo órdenes pagadas
    ventas = orders[orders['estado_orden'] == "Pagada"]

    total_clientes = customers['id_cliente'].nunique()
    clientes_compradores = ventas['id_cliente'].nunique()
    clientes_no_compradores = total_clientes - clientes_compradores

    compras_por_cliente = ventas.groupby("id_cliente")['id_orden'].count()
    clientes_1_compra = (compras_por_cliente == 1).sum()
    clientes_recurrentes = (compras_por_cliente > 1).sum()

    tasa_conversion = clientes_compradores / total_clientes * 100 if total_clientes > 0 else 0
    tasa_marketing = (customers['marketing_cat'] == "Si").mean() * 100

    ingresos_totales = ventas['precio_total'].sum()
    ingreso_prom_cliente = ingresos_totales / clientes_compradores if clientes_compradores > 0 else 0

    metrics = [
        ("👥 Total Clientes", f"{total_clientes:,}", "#1f77b4"),
        ("🚫 Nunca Compraron", f"{clientes_no_compradores:,}", "#1f77b4"),
        ("🛒 1 Compra", f"{clientes_1_compra:,}", "#1f77b4"),
        ("🔁 Recurrentes (2+)", f"{clientes_recurrentes:,}", "#1f77b4")
    ]

    return metrics

def kpis_por(customers, orders):
    # Filtrar solo órdenes pagadas
    ventas = orders[orders['estado_orden'] == "Pagada"]

    total_clientes = customers['id_cliente'].nunique()
    clientes_compradores = ventas['id_cliente'].nunique()
    clientes_no_compradores = total_clientes - clientes_compradores

    compras_por_cliente = ventas.groupby("id_cliente")['id_orden'].count()
    clientes_1_compra = (compras_por_cliente == 1).sum()
    clientes_recurrentes = (compras_por_cliente > 1).sum()

    tasa_conversion = clientes_compradores / total_clientes * 100 if total_clientes > 0 else 0
    tasa_marketing = (customers['marketing_cat'] == "Si").mean() * 100

    ingresos_totales = ventas['precio_total'].sum()
    ingreso_prom_cliente = ingresos_totales / clientes_compradores if clientes_compradores > 0 else 0
    imgreso_prom_totales= ingresos_totales/total_clientes

    metrics = [
        ("📈 Conversión a Compra", f"{tasa_conversion:.2f}%", "#1f77b4"),
        ("💌 Aceptan Marketing", f"{tasa_marketing:.2f}%", "#1f77b4"),
        ("💰 Ingreso Promedio por Cliente", f"${ingreso_prom_cliente:,.0f} CLP", "#1f77b4"),
        ("💰 Ingreso Promedio por Cliente Totales", f"${imgreso_prom_totales:,.0f} CLP", "#1f77b4"),
    ]

    return metrics

 #ARPU: Ingreso Promedio por Cliente Comprador
 #ARPC (Average Revenue per Customer) = ingresos / clientes registrados.


def geo_clientes(customer, ciudad_muni):
    if ciudad_muni== 0:
        columna= 'cliente_ciudad'
        label='Ciudad'
    elif ciudad_muni==1:
        columna='cliente_municipalidad'
        label='Municipalidad'

    clientes= customer.copy()

    # Limpiar valores nulos o 'Sin especificar'
    clientes = clientes[
        clientes[columna]
        .fillna("")
        .str.strip()
        .str.lower() != "sin especificar"
    ]

    clientes_ciudad = (
        clientes.groupby(f"{columna}")['id_cliente']
        .count().reset_index()
        .sort_values("id_cliente", ascending=False)
        .head(5)
    )
    fig_ciudad = px.bar(
        clientes_ciudad, x=f"{columna}", y="id_cliente",
        text_auto=True, labels={"id_cliente": "Nro Clientes", columna: label},
        color_discrete_sequence=[colores['purpura']] 
    )
    # Ajustes para que se vea bien en Streamlit deploy
    fig_ciudad.update_layout(
        autosize=True,
        margin=dict(l=40, r=20, t=30, b=40),
        height=400,
        yaxis=dict(tickfont=dict(size=12)),
        xaxis=dict(tickfont=dict(size=12))
    )

    st.plotly_chart(fig_ciudad, use_container_width=True)

def marketing_distribucion(customers):
    clientes_marketing = customers['acepta_marketing'].value_counts().reset_index()
    clientes_marketing.columns = ['acepta_marketing', 'conteo']
    fig_marketing = px.pie(
        clientes_marketing,
        names='acepta_marketing',
        values='conteo',
        hole=0.3,  # opcional, hace un donut
         color='acepta_marketing',
        color_discrete_map={
            'Sí': colores['verde_lima'],
            'No': colores['rojo'],
            1: colores['verde_lima'],
            0: colores['rojo']
        }
    )

    # Ajustes para Streamlit
    fig_marketing.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, t=30, b=20),
        height=400,  # ajustable según la cantidad de categorías
        legend=dict(font=dict(size=12))
    )

    st.plotly_chart(fig_marketing, use_container_width=True)

def marketing_conversion(customers, orders):
    """
    Calcula cuántos clientes que aceptan marketing realmente compran.
    """
    # Filtrar solo órdenes pagadas
    ventas = orders[orders['estado_orden'] == "Pagada"]

    # Clientes que compraron
    clientes_con_ventas = ventas['id_cliente'].unique()

    # Unir con clientes
    customers['comprador'] = customers['id_cliente'].isin(clientes_con_ventas)

    # Cruce entre marketing y si compró
    tabla = customers.groupby(['acepta_marketing', 'comprador'])['id_cliente'].count().reset_index()
    tabla.columns = ['acepta_marketing', 'comprador', 'conteo']

    fig_conv = px.bar(
        tabla,
        x="acepta_marketing",
        y="conteo",
        color="comprador",
        barmode="group",
        labels={
            "acepta_marketing": "Acepta Marketing",
            "conteo": "Número de Clientes",
            "comprador": "¿Compró?"
        },
        color_discrete_map={
            True:colores['verde_lima'],   # verde para los que compraron
            False:colores['rojo']   # rojo para los que no compraron
        }
    )
    # Ajustes para Streamlit
    fig_conv.update_layout(
        autosize=True,
        margin=dict(l=40, r=20, t=30, b=40),
        height=400,
        xaxis=dict(tickfont=dict(size=12)),
        yaxis=dict(tickfont=dict(size=12))
    )

    st.plotly_chart(fig_conv, use_container_width=True)

# ---- Top clientes por gasto y frecuencia ----
def gasto_frec(orders, customers):
    orders = orders[orders['estado_orden'] == "Pagada"]
    clientes_gasto_freq = (
        orders.groupby('id_cliente')
        .agg({
            'precio_total': 'sum',   # gasto total
            'id_orden': 'count'      # cantidad de compras
        })
        .reset_index()
        .rename(columns={'id_orden': 'numero_compras'})   
        .merge(customers[['id_cliente','cliente_nombre']], on='id_cliente', how='left')
        .sort_values('precio_total', ascending=False)
        .head(10)
    )

    # Mostrar tabla
    st.markdown("### Top 10 Mejores Clientes - Detalles ")
    st.dataframe(clientes_gasto_freq)

    st.markdown("### Top 10 Mejores Clientes - Gráfica ")
    # Gráfica combinada: gasto vs frecuencia
    fig_top = px.bar(
        clientes_gasto_freq,
        x='cliente_nombre',
        y='precio_total',
        text='numero_compras',   # <-- usamos el nuevo nombre
        labels={
            "cliente_nombre": "Cliente",
            "precio_total": "Gasto Total",
            "numero_compras": "Número de Compras"
        },
        color_discrete_sequence=[colores['verde']]  
    )
    fig_top.update_layout(
        autosize=True,
        margin=dict(l=40, r=20, t=30, b=80),  # margen inferior más grande para tickangle
        height=500,  # ajusta según cantidad de clientes
        xaxis=dict(tickfont=dict(size=12)),
        yaxis=dict(tickfont=dict(size=12))
    )

    fig_top.update_xaxes(tickangle=45)
    fig_top.update_traces(textposition='outside')

    st.plotly_chart(fig_top, use_container_width=True)
# ------------------------------------------ CLIENTES ----------------
# load_css()
# kreadores_header()

# # ---- Configuración de página ----
# # st.set_page_config(page_title="Clientes", layout="wide")
# st.markdown('<h2 class="section-title">Clientes</h2><br>', unsafe_allow_html=True)

# # ---- Recuperar los DataFrames ----
# df_clean = st.session_state.df_clean
# customers = df_clean['customers_clean']
# orders = df_clean['orders_clean']
# # hacer un try para obtener los df, si hay error, decir primero recargar pagina de Inicio para que se cargen los df
# # AttributeError: st.session_state has no attribute "df_clean". Did you forget to initialize it?

# # ---- KPIs ----
# metrics = get_clientes_kpis(customers, orders)
# display_metrics(metrics)

# metrics_por = kpis_por(customers, orders)
# display_metrics(metrics_por)

# # ---- Top 5 ciudades ----
# st.markdown('<h3 class="section-title">📊 Distribución Geográfica</h3>', unsafe_allow_html=True)

# st.markdown("### Top 5 Ciudades con más Clientes")
# geo_clientes(customers,0)
# st.markdown("###  Top 5 Municipalidades con más Clientes")
# geo_clientes(customers,1)


# # st.markdown("### 📊 Segmentación de Clientes")
# st.markdown('<h3 class="section-title">📊 Segmentación de Cliente</h3>', unsafe_allow_html=True)
# # ---- Marketing ----
# col1, col2 = st.columns(2)

# with col1:
#     st.markdown("### Clientes que Aceptan Marketing")
#     marketing_distribucion(customers)
# with col2:
#     st.markdown("### Conversión de Clientes según Marketing")
#     marketing_conversion(customers, orders)

# # st.markdown("### 🏆 Top Clientes")
# st.markdown('<h3 class="section-title">🏆 Top Clientes</h3>', unsafe_allow_html=True)
# gasto_frec(orders, customers)

