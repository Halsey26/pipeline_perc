
# ETL Pipeline - Perceivo eCommerce Agent

Este módulo contiene la implementación del **pipeline ETL (Extract, Transform, Load)** para el agente conversacional de eCommerce.  
El objetivo es automatizar la extracción de datos desde la API de Jumpseller, transformarlos para análisis y modelos de ML, y cargarlos en **Supabase** para su posterior uso en dashboards (Streamlit) y orquestación con **Prefect**.

---

## 📂 Estructura del directorio
```
etl (actual carpeta)
│── init.py
│── pruebas_extract.ipynb 
│── pruebas_transform.ipynb 
│── pruebas_load.ipynb 
│── extract.py 
│── transform.py 
│── load.py 
```


```
prefect_flows/
│── init.py
│── main.py # Orquestador del pipeline (llama a extract, transform, load con Prefect)
```

---

## 📝 Descripción de archivos

### **Notebooks de pruebas**
- **pruebas_extract.ipynb** → Testeo de conexión a la API de Jumpseller, extracción de *products*, *customers* y *orders*. Validación de formatos y almacenamiento inicial.  
- **pruebas_transform.ipynb** → Limpieza de datos, creación de columnas derivadas (ej. recency, frequency, monetary), prueba de features para modelos como propensión de recompra o churn.  
- **pruebas_load.ipynb** → Validación de conexión con Supabase, inserción de data procesada en tablas (`customers_clean`, `orders_clean`, etc.).  

### **Scripts finales**
- **extract.py** →  (API Jumpseller → data/raw) <br>
  Contiene funciones productivas para obtener datos desde Jumpseller.
  - Guarda una copia cruda en `data/raw/`.  
  - Devuelve un DataFrame para el flujo.  
- **transform.py** → (data/raw → procesado) <br>
  Contiene funciones productivas para transformar y preparar datos para análisis/modelos. 
- **load.py** → (procesado → Supabase) <br>
  Funciones para cargar datos transformados en **Supabase**.  

---

## 🔄 Flujo del pipeline ETL

1. **Extract**  
   - API de Jumpseller → datos crudos (`products`, `customers`, `orders`).  
   - Guardados como `parquet` en `data/raw/` y enviados a Supabase (tablas raw).  

2. **Transform**  
   - Lectura de datos crudos desde Supabase.  
   - Limpieza, normalización y creación de nuevas variables.  
   - Resultados preparados para análisis/modelos.  

3. **Load**  
   - Inserción de datasets limpios/procesados en Supabase.  
   - Estos datos alimentan dashboards en Streamlit y modelos ML.  

---

## ⚙️ Tecnologías utilizadas

- **Python (pandas, requests, dotenv)** → procesamiento y manejo de datos.  
- **Prefect** → orquestación de los pipelines y flujos de datos.  
- **Supabase (Postgres)** → almacenamiento de datos en crudo (`raw`) y limpios (`clean`).  
- **Streamlit** → visualización de métricas y modelos en dashboards.  

---

## 🚀 Próximos pasos

- Implementar modelos de **propensión de recompra** y **churn** como parte de la capa de transformación.  
- Modularizar pipelines de métricas y features para futuros modelos.  
- Extender la carga a dashboards de negocio con métricas clave: crecimiento de clientes, retención, ticket promedio.  
- Documentar monitoreo de pipelines y costos asociados a Supabase/Prefect en producción.  

---

✍️ **Nota**: Este README describe el módulo `etl`. Para más detalles sobre la integración con el orquestador (`prefect_flows/`) y la aplicación final, ver el README principal del repositorio.
