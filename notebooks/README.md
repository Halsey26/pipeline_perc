
# ETL Pipeline - Perceivo eCommerce Agent

Este módulo contiene los notebooks realizados para la implementación del **pipeline ETL (Extract, Transform, Load)** para el agente conversacional de eCommerce.  
---

## 📂 Estructura del directorio
```
etl (actual carpeta)
│── init.py
│── pruebas_extract.ipynb 
│── pruebas_transform_load.ipynb 
│── pruebas_dashboard.ipynb 
```

---

## 📝 Descripción de archivos

### **Notebooks de pruebas**
- **pruebas_extract.ipynb** → Testeo de conexión a la API de Jumpseller, extracción de *products*, *customers* y *orders*. Validación de formatos y almacenamiento inicial.  
- **pruebas_transform_load.ipynb** 
  - → Limpieza de datos, creación de columnas derivadas (ej. recency, frequency, monetary), prueba de features para modelos como propensión de recompra o churn.  
  - → Validación de conexión con Supabase, inserción de data procesada en tablas (`customers_clean`, `orders_clean`, etc.).  
-  **pruebas_dashboard.ipynb** 

---

## ⚙️ Tecnologías utilizadas

- **Python (pandas, requests, dotenv)** → procesamiento y manejo de datos.  
- **Supabase (Postgres)** → almacenamiento de datos en crudo (`raw`) y limpios (`clean`).  
- **Streamlit** → visualización de métricas y modelos en dashboards.  

---

## 🚀 Próximos pasos

- Implementar modelos de **propensión de recompra** y **churn** como parte de la capa de transformación.  
- Modularizar pipelines de métricas y features para futuros modelos.  
- Extender la carga a dashboards de negocio con métricas clave: crecimiento de clientes, retención, ticket promedio.  
- Documentar monitoreo de pipelines y costos asociados a Supabase/Prefect en producción.  

---

✍️ **Nota**: Este README describe el módulo `notebooks`. Para más detalles sobre la integración con el orquestador (`prefect_flows/`) y la aplicación final, ver el README principal del repositorio.
