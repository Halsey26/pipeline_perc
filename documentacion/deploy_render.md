# Deploy en Render — Guía completa

Archivo: `documentacion/deploy_render.md`

Guía detallada para desplegar la aplicación Streamlit (y recomendaciones relacionadas) en Render. Está pensada para integrarse en `documentacion/` del repo.

- Nota: En este caso se uso render para el deploy, pero puedes usar otro de tu preferencia(Railway, Streamlit Cloud, etc)

---

## 1. Resumen rápido (configuración que ya tienes)

* **Branch**: `main`
* **Root directory**: `.` (si `requirements.txt` está en la raíz)
* **Build command**: `pip install --no-cache-dir -r requirements.txt`
* **Start command**: `streamlit run streamlit_app/Inicio.py --server.port $PORT --server.address 0.0.0.0 --server.headless true`

> Recomendación: usa `--no-cache-dir` en `pip install` durante el build para reducir el uso de espacio en disco durante la instalación.

---

## 2. Conectar el repo y crear el servicio

1. En Render: **New → Web Service**.
2. Conectar con tu repositorio (GitHub/GitLab) y seleccionar la rama `main`.
3. Configurar:

   * **Name**: nombre descriptivo (ej. `perceivo-dashboard`).
   * **Root directory**: `.` (si `requirements.txt` está en la raíz).
   * **Build command**: `pip install --no-cache-dir -r requirements.txt` (ver nota sobre “peso” abajo).
   * **Start command**: `streamlit run streamlit_app/Inicio.py --server.port $PORT --server.address 0.0.0.0 --server.headless true`.
   * **Runtime**: seleccionar la versión de Python que uses (p. ej. `Python 3.11`).
4. Crear y desplegar. Activar **Auto Deploy** para que los commits al branch `main` desencadenen nuevos deploys.

---

## 3. Variables de entorno y secretos

En el panel de **Environment** de tu servicio en Render, agrega todas las variables sensibles como **Environment Variables** (no en el repo):

* `SUPABASE_URL` = `https://...` (tu placeholder real en entorno)
* `SUPABASE_KEY` = `eyJ...` (anon o service role según lo necesites)
* `JUMPSELLER_LOGIN` = `...`
* `JUMPSELLER_AUTHTOKEN` = `...`
* `PGUSER`, `PGPASSWORD`, `PGHOST`, `PGPORT`, `PGDATABASE` (si usas conexión Postgres directa)
* `PORT` no hace falta establecerla (Render la establece automáticamente en la variable `$PORT` que tu start command debe usar)

**Consejo:** usa las opciones de Render para marcar variables como *Secret* si el panel lo permite.

---

## 4. Manejo del puerto en Streamlit (problema común)

Render asigna el puerto en la variable de entorno `$PORT`. Streamlit por defecto no escucha esa variable, así que tu comando de inicio debe pasar `$PORT` a Streamlit. Ejemplos:

**Start command (directo):**

```
streamlit run streamlit_app/Inicio.py --server.port $PORT --server.address 0.0.0.0 --server.headless true
```

**Start command (script wrapper):** crear `start_streamlit.sh` en la raíz del repo con:

```sh
#!/usr/bin/env bash
set -e

# Si $PORT no está definido, usar 8501 por defecto (útil localmente)
: ${PORT:=8501}

streamlit run streamlit_app/Inicio.py \
  --server.port $PORT \
  --server.address 0.0.0.0 \
  --server.headless true
```

Hacer ejecutable el script: `chmod +x start_streamlit.sh` y usar `./start_streamlit.sh` como Start Command.

---

## 5. Evitar fallos por "peso" (dependencias grandes)

Problemas comunes: tiempo de build largo, límites de memoria/disk durante build, paquetes binarios pesados.

Recomendaciones:

* **Probar localmente en entorno limpio**: crea un virtualenv nuevo e instala `pip install --no-cache-dir -r requirements.txt` para medir tiempos y posibles errores.
* **Revisar `requirements.txt`**:

  * Elimina dependencias no usadas.
  * Si tienes librerías pesadas (Torch, TensorFlow, etc.) considera moverlos a un servicio separado (microservicio o servicio GPU) o usar ruedas precompiladas específicas.
  * Prefiere librerías con wheels en PyPI (evita builds desde fuente si no es necesario).
* **Usar una imagen Docker** (opción alternativa): si necesitas control total de entorno/paquetes, crea un `Dockerfile` y selecciona deploy por Docker en Render (útil si necesitas dependencias nativas específicas).
* **Instalación con caches limitados**: `pip install --no-cache-dir` reduce almacenamiento intermedio durante build.
* **Separar responsabilidades**: mover tareas de ML/entrenamiento a workers o servicios especializados (ej. un worker en Render o en un proveedor de ML).

---

## 6. Tamaño de archivos estáticos y assets

* Las imágenes grandes o assets estáticos no deben incluirse en el repo si son pesadas. Usa:

  * Supabase Storage
  * Cloud Storage (GCS, S3)
  * CDN
* Si incluyes capturas/activos en la app, servirlos desde Supabase Storage o un bucket público reducirá el tamaño del repo y acelerará el deploy.

---

## 7. Logs, monitoreo y salud

* Revisa los logs en Render (Build logs y Instance logs) para diagnosticar fallos de build o runtime.
* Añade endpoints o checks para salud si necesitas que Render valide la app:

  * Streamlit no tiene un endpoint HTTP de salud por defecto; puedes crear un pequeño endpoint aparte (por ejemplo con FastAPI) si necesitas health checks más avanzados.
* Habilita alertas en Render (si tu plan lo permite) o configura un canal de notificaciones (Slack/Email) para fallos de deploy.

---

## 8. Prefect / Orquestación y Background jobs

* Si tu pipeline ETL usa Prefect y requiere un agent en ejecución, considera:

  * Ejecutar Prefect agent en una instancia separada (puede ser otro servicio en Render o un servidor/VM aparte).
  * Usar Prefect Cloud (o Prefect Orion) y ejecutar flows en cloud/agent remoto.
* No intentes ejecutar orquestadores y la app Streamlit en el mismo servicio si las cargas son pesadas o requieren persistencia/cron; mejor separar en servicios: Web Service (dashboard) + Background Worker (ETL / Prefect agent).

---

## 9. CI/CD y despliegues automáticos

* Render soporta despliegues automáticos al hacer push en `main`. Para mayor control puedes:

  * Crear un pipeline en GitHub Actions que ejecute tests y linters antes de merge.
  * Añadir pasos para compilar assets (si los tienes) y generar artefactos.
* Mantén `requirements.txt` y un `python-version` o `runtime.txt` para reproducibilidad.

---

## 10. Troubleshooting (problemas frecuentes y cómo solucionarlos)

* **Build fallido por memoria / tiempo**:

  * Reducir dependencias; usar `--no-cache-dir`; pasar a Docker si necesitas control.
* **App no responde / Error de puerto**:

  * Verifica que el Start Command use `$PORT` y `--server.address 0.0.0.0`.
* **Secretos no funcionan**:

  * Asegúrate de definir las variables de entorno en Render (Environment) y de que los nombres coincidan con los esperados por la app.
* **Errores en import de librerías nativas** (ej. `numpy`, `pandas` con versiones incompatibles)

  * Forzar versiones compatibles en `requirements.txt` o usar `pip wheel`/Docker.

---

## 11. Alternativa: Deploy con Docker (resumen)

Si tu `requirements.txt` contiene librerías que compilan desde fuente o necesitas control sobre la imagen, usar Docker te da más flexibilidad.

Ejemplo mínimo de `Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PORT=8501
EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app/Inicio.py", "--server.port", "${PORT}", "--server.address", "0.0.0.0", "--server.headless", "true"]
```

En Render, selecciona "Docker" como método de deploy y sube tu Dockerfile.

---

## 12. Checklist final antes de conectar a Render

* [ ] `requirements.txt` revisado y probado en un entorno limpio.
* [ ] `.env` con placeholders incluido en el repo (sin secretos reales).
* [ ] Scripts de inicio (`start_streamlit.sh`) y permisos correctos si usas wrapper.
* [ ] Variables de entorno/secretos configuradas en Render.
* [ ] Branch `main` listo y con deploy automático activado.
* [ ] Plan para separar Prefect / ETL si aplica (servicio separado o Prefect Cloud).
