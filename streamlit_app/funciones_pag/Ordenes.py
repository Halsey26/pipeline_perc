import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from funciones_pag.Inicio import kreadores_header , display_metrics
from estilos import load_css


# ------------ FUNCIONES
def kpis_ordenes(orders):
    # Total de órdenes
    total_ordenes = len(orders)

    # Órdenes según estado
    ordenes_pagadas = orders[orders['estado_orden'] == "Pagada"]
    ordenes_abandonadas = orders[orders['estado_orden'] == "Abandonada"]
    ordenes_por_pagar=  orders[orders['estado_orden'] == "Pendiente de pago"]

    n_pagadas = len(ordenes_pagadas)
    n_abandonadas = len(ordenes_abandonadas)
    n_pendiente_pago = len(ordenes_por_pagar)

    # Empaquetamos las métricas
    metrics = [
        ("📦 Total Órdenes", f"{total_ordenes:,}", "#1f77b4"),
        ("✅ Órdenes Pagadas", f"{n_pagadas:,}", "#2ca02c"),
        ("⏳ Órdenes Pendientes por Pagar", f"{n_pendiente_pago:,}", "#B4AB05"),
        ("❌ Órdenes Abandonadas", f"{n_abandonadas:,}", "#d62728"),
    ]

    return metrics

def kpis_porc(orders):
    # Total de órdenes
    total_ordenes = len(orders)

    # Órdenes según estado
    ordenes_pagadas = orders[orders['estado_orden'] == "Pagada"]
    n_pagadas = len(ordenes_pagadas)
   
    # KPIs de conversión e ingresos
    tasa_conversion = (n_pagadas / total_ordenes * 100) if total_ordenes > 0 else 0
    ingresos_totales = ordenes_pagadas['precio_total'].sum()
    ticket_promedio = ingresos_totales / n_pagadas if n_pagadas > 0 else 0

    # Cumplimiento y logística
    cumplimiento_pct = (
        orders['estado_cumplimiento'].value_counts(normalize=True).get("Cumplido", 0) * 100
    )
    entregado_pct = (
        orders['estado_envio'].value_counts(normalize=True).get("Entregado", 0) * 100
    )

    # Empaquetamos las métricas
    metrics = [
        ("📈 Tasa de Conversión", f"{tasa_conversion:.1f}%", "#9467bd"),
        ("💰 Ingresos Totales", f"${ingresos_totales:,.0f}", "#ff7f0e"),
        ("🧾 Ticket Promedio", f"${ticket_promedio:,.2f}", "#8c564b"),
        ("📌 Cumplimiento", f"{cumplimiento_pct:.1f}%", "#17becf"),
        ("🚚 Entregadas", f"{entregado_pct:.1f}%", "#bcbd22"),
    ]

    return metrics

# ---- Órdenes e Ingresos en el tiempo ----
def grafica_tiempo(orders):
    orders['fecha_creacion'] = pd.to_datetime(orders['fecha_creacion'])

    # Filtrar solo órdenes pagadas (opcional)
    ventas = orders[orders['estado_orden'] == "Pagada"]

    ordenes_time = ventas.groupby(ventas['fecha_creacion'].dt.to_period("M")).agg({
        "id_orden": "nunique",
        "precio_total": "sum"
    }).reset_index()
    ordenes_time['fecha_creacion'] = ordenes_time['fecha_creacion'].astype(str)

    # Calcular crecimiento % mes a mes
    ordenes_time['ordenes_pct_change'] = ordenes_time['id_orden'].pct_change() * 100
    ordenes_time['ingresos_pct_change'] = ordenes_time['precio_total'].pct_change() * 100

    # Línea doble eje: Órdenes vs Ingresos
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])

    fig1.add_trace(
        go.Bar(x=ordenes_time['fecha_creacion'], y=ordenes_time['id_orden'],
            name="Órdenes", marker_color="#1f77b4"),
        secondary_y=False
    )

    fig1.add_trace(
        go.Scatter(x=ordenes_time['fecha_creacion'], y=ordenes_time['precio_total'],
                name="Ingresos", mode="lines+markers", line=dict(color="#ff7f0e")),
        secondary_y=True
    )

    # Personalización
    st.markdown("### Órdenes e Ingresos en el Tiempo")
    fig1.update_layout(
        xaxis_title="Mes",
        yaxis_title="Órdenes",
        legend=dict(orientation="h", y=-0.2),  # leyenda horizontal debajo del gráfico
        hovermode="x unified",
        autosize=True,
        margin=dict(l=40, r=20, t=30, b=60),
        height=450,  # ajustable según cantidad de meses
        xaxis=dict(tickfont=dict(size=12)),
        yaxis=dict(tickfont=dict(size=12))
    )
    fig1.update_yaxes(title_text="Órdenes", secondary_y=False)
    fig1.update_yaxes(title_text="Ingresos (CLP)", secondary_y=True)

    st.plotly_chart(fig1, use_container_width=True)

    # ---- Crecimiento mes a mes ----
    st.markdown("### Crecimiento % Mes a Mes")
    fig2 = px.bar(
        ordenes_time,
        x="fecha_creacion",
        y=["ordenes_pct_change", "ingresos_pct_change"],
        barmode="group",
        # title="📈 Crecimiento % Mes a Mes",
        labels={"value": "% Crecimiento", "fecha_creacion": "Mes", "variable": "Métrica"}
    )
    fig2.update_layout(
    autosize=True,
    margin=dict(l=40, r=20, t=30, b=60),
    height=450,
    legend=dict(orientation="h", y=-0.2),
    xaxis=dict(tickfont=dict(size=12)),
    yaxis=dict(tickfont=dict(size=12)),
    hovermode="x unified"
    )
    st.plotly_chart(fig2, use_container_width=True)

def plot_estado_orden(orders):
    """
    Genera un gráfico circular de la distribución de estados de órdenes.
    """
    # --- Agrupar y calcular proporciones ---
    estado_counts = (
        orders['estado_orden']
        .value_counts(dropna=False)
        .reset_index()
    )
    estado_counts.columns = ['estado_orden', 'conteo']
    estado_counts['porcentaje'] = estado_counts['conteo'] / estado_counts['conteo'].sum() * 100

    # --- Gráfico ---
    st.markdown("### Distribución de Estado de Órdenes")
    fig = px.pie(
        estado_counts,
        names="estado_orden",
        values="conteo",
        hole=0.3,  # donut chart
        color="estado_orden",
        color_discrete_map={
            "Pagada": "#2ca02c",
            "Abandonada": "#ff7f0e",
            "Cancelada": "#d62728",
            "Pendiente de pago": "#9467bd",
            "Creada": "#1f77b4"
        }
    )

    # Mostrar % y conteo en etiquetas
    fig.update_traces(
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>Órdenes: %{value}<br>Porcentaje: %{percent}"
    )
    # Ajustes de layout para Streamlit
    fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, t=30, b=20),
        height=400,  # ajustable según cantidad de categorías
        legend=dict(font=dict(size=12))
    )

    st.plotly_chart(fig, use_container_width=True)

def plot_cumplimiento(orders):
    """
    Genera un gráfico de barras de la distribución de estado de cumplimiento de órdenes.
    """
    cumplimiento = (
        orders.groupby("estado_cumplimiento")["id_orden"]
        .count()
        .reset_index()
        .sort_values("id_orden", ascending=False)
    )
    st.markdown("### Distribución de Estado de Cumplimiento")
    fig = px.bar(
        cumplimiento,
        x="estado_cumplimiento",
        y="id_orden",
        text="id_orden",
        color="estado_cumplimiento",
        color_discrete_map={
            "Cumplido": "#2ca02c",      # verde
            "No cumplido": "#d62728",   # rojo
        }
    )

    fig.update_layout(
        xaxis_title="Estado de Cumplimiento",
        yaxis_title="Número de Órdenes",
        uniformtext_minsize=10,
        uniformtext_mode="hide",
        autosize=True,
        margin=dict(l=40, r=20, t=30, b=40),
        height=400,  # ajustable según cantidad de categorías
        xaxis=dict(tickfont=dict(size=12)),
        yaxis=dict(tickfont=dict(size=12)),
        showlegend=True
    )
    st.plotly_chart(fig, use_container_width=True)


# def distri_empresa(orders):
#     empresa = orders.groupby("empresa_envio")["id_orden"].count().reset_index()
#     st.markdown("### Órdenes por Empresa de Envío")
#     fig = px.bar(empresa, x="empresa_envio", y="id_orden")
#     st.plotly_chart(fig, use_container_width=True)

def distri_empresa(orders):
    """
    Genera un gráfico de barras mostrando la distribución de órdenes
    por empresa de envío.
    """
    empresa = (
        orders.groupby("empresa_envio")["id_orden"]
        .count()
        .reset_index()
        .sort_values("id_orden", ascending=False)
    )
    st.markdown("### 🚚 Órdenes por Empresa de Envío")
    fig = px.bar(
        empresa,
        x="empresa_envio",
        y="id_orden",
        text="id_orden",
        color="empresa_envio",
        color_discrete_sequence=px.colors.qualitative.Set2
    )

    fig.update_traces(
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Órdenes: %{y:,}"
    )
    fig.update_layout(
        xaxis_title="Empresa de Envío",
        yaxis_title="Número de Órdenes",
        uniformtext_minsize=10,
        uniformtext_mode="hide",
        autosize=True,
        margin=dict(l=40, r=20, t=30, b=60),
        height=400,  # ajustable según cantidad de empresas
        xaxis=dict(tickfont=dict(size=12)),
        yaxis=dict(tickfont=dict(size=12)),
        showlegend=False
    )
    st.plotly_chart(fig, use_container_width=True)

def top_regiones(orders):
    # Filtrar solo las órdenes pagadas
    ventas = orders[orders['estado_orden'] == "Pagada"]

    # Contar órdenes por región
    top_regiones = (
        ventas.groupby("region_envio")["id_orden"]
        .count()
        .reset_index()
        .nlargest(10, "id_orden")
    )

    # Gráfico de barras
    fig = px.bar(
        top_regiones,
        x="region_envio",
        y="id_orden",
        title="Top 10 Regiones con más Órdenes Pagadas",
        text_auto=True,
        color="region_envio",
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig.update_layout(
        title="Top 10 Regiones con más Órdenes Pagadas",
        xaxis_title="Región de Envío",
        yaxis_title="Número de Órdenes",
        autosize=True,
        margin=dict(l=40, r=20, t=50, b=60),
        height=450,
        xaxis=dict(tickfont=dict(size=12)),
        yaxis=dict(tickfont=dict(size=12)),
        showlegend=False
    )
    # Mostrar en Streamlit
    st.markdown("### Clientes - Top Regiones con más Órdenes")
    st.plotly_chart(fig, use_container_width=True)
# -------------------------------- ORDENES -----------------------------
# load_css()
# kreadores_header()

# # ---- Configuración de página ----

# st.markdown('<h2 class="section-title"> Órdenes</h2><br>', unsafe_allow_html=True)

# # Recuperas los dfs cargados en Home
# df_clean = st.session_state.df_clean

# customers = df_clean['customers_clean']
# orders = df_clean['orders_clean']

# # ---- KPIs ----
# metrics = kpis_ordenes( orders)
# display_metrics(metrics)

# metrics_por = kpis_porc( orders)
# display_metrics(metrics_por)


# # ---- Órdenes en el tiempo ----
# grafica_tiempo(orders)

# col1, col2 = st.columns(2)

# with col1:
#     # ---- Estado de órdenes ----
#     plot_estado_orden(orders)    
    
# with col2:
#     # ---- Estado cumplimiento/envío ----
#     plot_cumplimiento(orders)


# # ---- Empresa de envío ----
# distri_empresa(orders)

# # ---- Top 10 regiones ----
# top_regiones(orders)