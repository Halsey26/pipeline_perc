import streamlit as st

# --- Paleta de la marca Kreadores ---

colores = {
    # Identidad principal
    "dorado_corpo": "#D9A607",    # Dorado corporativo
    "dorado_prim": "#D97706", # PRIMARY_COLOR
    "negro": "#000000",     # Negro del logo
    "blanco": "#FFFFFF",    # Blanco neutro

    # Neutros para fondos y texto
    "blanco_tarjeta": "#FFFFFF",   # Blanco puro para tarjetas CARD_BACKGROUND
    "gris_borde": "#E5E7EB",        # Gris claro para bordes BORDER_COLOR
    "gris_claro": "#F8F9FA",  # Fondo claro
    "gris_muy_claro": "#F9FAFB",   # Fondo gris muy claro BACKGROUND_COLOR
    "gris_medio": "#A0AEC0",  # Neutro medio
    "gris_oscuro": "#2D3748", # Texto oscuro
    "text_gris_oscuro": "#1F2937",      # Texto gris oscuro TEXT_COLOR

    # Accentos / gráficos
    "azul": "#1E3A8A",        # Azul confianza
    "azul_claro": "#3B82F6",  # Azul vibrante
    "azul_medio": "#667eea",  # Azul principal
    "purpura": "#B598D2",     # Púrpura secundario
    "rosa": "#F093FB",        # Rosa acento
    "rosa_magenta": "#EC4899", #ACCENT_COLOR
    "verde": "#48BB78",       # Verde éxito
    "verde_lima":  "#10B981" , #SECONDARY_COLOR
    "naranja": "#ED8936",     # Naranja advertencia
    "rojo": "#F56565",        # Rojo peligro
    "rojo_vibrante": "#EF4444",    # Rojo vibrante DANGER_COLOR
    "amarillo": "#F6E05E",    # Amarillo apoyo (más suave que el dorado)
    "dorado_claro": "#F0C75E", # Variante dorado más clara
    "dorado_oscuro": "#B8860B", # Variante dorado más oscura
    "amarillo_vibrante": "#F59E0B",# Amarillo/naranja vibrante WARNING_COLOR 
    "dorado_medio": "#FBBF24"

}
##080003
##9F9FB5

def load_css(colores):
    st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .main-header {{
        background: linear-gradient(90deg,{colores['negro']} 0%, {colores['dorado_medio']} 100%);
        padding: 2.5rem 1rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        font-family: 'Inter', sans-serif;
    }}

    .camera-icon {{
        font-size: 80px;
        color: #FFD700;
        margin-bottom: 10px;
        display: block;
        line-height: 1;
    }}

    .kreadores-logo{{
        font-size: 3rem;
        font-weight: 900;
        margin-bottom: 5px;
        background: linear-gradient(45deg, #FFD700, #FFA500);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-shadow: 2px 2px 6px rgba(0,0,0,0.3);
        display: block;
    }}

    .tagline {{
        font-size: 1.5rem;
        opacity: 0.9;
        font-weight: 350;
        display: block;
        margin-top: 0; 
    }}
   .subtagline {{
        font-size: 1.1rem;
        opacity: 0.9;
        font-weight: 250;
        display: block;
        margin-top: 0; 
    }}
    .metric-card {{
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-radius: 15px;
        padding: 25px;
        box-shadow: 0 8px 25px rgba(0,0,0,0.1);
        text-align: center;
        margin-bottom: 20px;
        transition: all 0.3s ease;
        border-left: 5px solid #667eea;
    }}
    .metric-card:hover {{
        transform: translateY(-8px);
        box-shadow: 0 15px 35px rgba(0,0,0,0.15);
    }}
    .metric-value {{
        font-size: 32px;
        font-weight: 700;
        color: #667eea;
        margin: 15px 0;
        font-family: 'Inter', sans-serif;
    }}
    .metric-label {{
        font-size: 16px;
        color: #495057;
        font-weight: 500;
        font-family: 'Inter', sans-serif;
    }}
    .section-title {{
        border-bottom: 3px solid #667eea;
        padding-bottom: 15px;
        margin-top: 30px;
        margin-bottom: 30px;
        color: #667eea;
        font-size: 28px;
        font-weight: 600;
        font-family: 'Inter', sans-serif;
    }}
    .stRadio > div {{
        flex-direction: row !important;
        gap: 20px;
    }}
    .stRadio label {{
        padding: 15px 30px;
        border-radius: 25px;
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        margin-right: 20px !important;
        transition: all 0.3s ease;
        font-weight: 500;
        border: 2px solid transparent;
        font-family: 'Inter', sans-serif;
    }}
    .stRadio label:hover {{
        background: linear-gradient(135deg, #e9ecef 0%, #dee2e6 100%);
        border-color: #667eea;
    }}
    .stRadio [data-baseweb="radio"]:checked + div {{
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        color: white !important;
        border-color: #667eea !important;
    }}
    .stPlotlyChart {{
        border-radius: 15px;
        box-shadow: 0 8px 25px rgba(0,0,0,0.08);
        padding: 20px;
        background: white;
        border: 1px solid #e9ecef;
    }}
    .recommendation-card {{
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 25px;
        border-radius: 20px;
        margin: 20px 0;
        box-shadow: 0 8px 30px rgba(0,0,0,0.2);
    }}
    .priority-high {{
        border-left: 6px solid #dc3545;
        background: linear-gradient(135deg, #fff5f5 0%, #ffe6e6 100%);
        padding: 20px;
        margin: 15px 0;
        border-radius: 12px;
        box-shadow: 0 4px 15px rgba(220,53,69,0.1);
    }}
    .priority-medium {{
        border-left: 6px solid #ffc107;
        background: linear-gradient(135deg, #fffbf0 0%, #fff3cd 100%);
        padding: 20px;
        margin: 15px 0;
        border-radius: 12px;
        box-shadow: 0 4px 15px rgba(255,193,7,0.1);
    }}
    .priority-low {{
        border-left: 6px solid #28a745;
        background: linear-gradient(135deg, #f0fff4 0%, #d4edda 100%);
        padding: 20px;
        margin: 15px 0;
        border-radius: 12px;
        box-shadow: 0 4px 15px rgba(40,167,69,0.1);
    }}
    .insight-box {{
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border: 2px solid #dee2e6;
        border-radius: 15px;
        padding: 25px;
        margin: 20px 0;
        box-shadow: 0 5px 20px rgba(0,0,0,0.05);
    }}
    .action-item {{
        background: white;
        border: 2px solid #dee2e6;
        border-radius: 12px;
        padding: 20px;
        margin: 15px 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
    }}
    .action-item:hover {{
        border-color: #667eea;
        box-shadow: 0 6px 20px rgba(0,0,0,0.1);
    }}

    .sidebar .sidebar-content {{
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }}
    </style>
    """, unsafe_allow_html=True)
