import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
# Retroceder una carpeta
parent_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))
ruta= os.path.join(parent_dir,"streamlit_app",'modelo', "modelo_churn.pkl")
print(BASE_DIR)
print(ruta)

import joblib

# # Ruta actual del archivo .py
# BASE_DIRBASE_DIR = os.path.dirname(os.path.abspath(__file__))
# # Retroceder una carpeta
# parent_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))

# ruta= os.path.join(BASE_DIR,"images", "logo_kreadores.png")

# # Entrar a otra carpeta (por ejemplo "etl")
# target_dir = os.path.join(parent_dir, "etl")

# # Añadir al sys.path
# sys.path.append(target_dir)
