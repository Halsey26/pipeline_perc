# Soporte operativo — Documentación robusta

> Archivo: `documentacion/soporte-operativo.md`
>
> Propósito: proveer una guía completa y robusta para el soporte, operación y respuesta a incidentes del proyecto (pipeline ETL, Supabase, Prefect, Streamlit, despliegue en Render). Pensado para que un nuevo integrante pueda usarlo como referencia y para que sirva como runbook durante incidentes.

---

## 1. Público y alcance

**Público:** Soporte técnico, SRE/DevOps de primer y segundo nivel, desarrolladores del proyecto.

**Alcance:**

* Operación y soporte del pipeline ETL (extract from Jumpseller → transform → load to Supabase).
* Base de datos Supabase/Postgres (esquemas `raw`, `clean`, `public` views).
* Orquestador Prefect (flows, agents, deployments).
* Dashboard Streamlit (deploy en Render).
* Tareas operativas: backups, despliegues, restauraciones, escalado, monitorización.

**No incluido:** operación de infra externa (p. ej. gestión de cuentas de Render/Supabase fuera de lo descrito), administración de redes corporativas.

---

## 2. Modelo de soporte y responsabilidades

**Niveles de soporte:**

* **L0 (Soporte básico / FAQs):** preguntas de documentación, reinicios simples, revisar guías. (Equipo de producto / helpdesk)
* **L1 (Soporte operacional):** revisar logs, reiniciar servicios, ejecutar runbooks estándar. (Soporte/Dev)
* **L2 (Soporte avanzado / Dev):** investigar bugs, corregir código, ejecutar restauraciones, reproducir errores localmente. (Desarrolladores)
* **L3 (Escalada / Arquitectura):** decisiones de diseño, cambios en infra e integración con proveedores. (Líder técnico / Arquitecto)

**Responsabilidades:**

* Mantener esta documentación actualizada.
* Mantener los runbooks y scripts en `documentacion/scripts/` y `documentacion/sql/`.
* Realizar rotación de backups y verificaciones periódicas.

---

## 3. Contactos y matriz de escalado

* **Contacto primario (Soporte L1):** Nombre — correo — teléfono/Slack
* **Contacto secundario (Soporte L2):** Nombre — correo — teléfono/Slack
* **Contacto arquitectura (L3):** Nombre — correo — teléfono/Slack
* **Soporte proveedor (Render):** enlace al soporte de Render / número de ticket
* **Soporte proveedor (Supabase):** enlace al soporte de Supabase

> **Plantilla:** Añadir la matriz real con horarios (timezone), on-call y reemplazos.

---

## 4. SLA y clasificación de severidad

**Severidad (ejemplos y tiempos objetivo)**

* **SEV-1 (Crítico):** Dashboard totalmente inaccesible para todos los usuarios o pipeline detenido que impide ventas/operaciones. *Respuesta inicial: 15 min*, *Resolver/mitigar: 4 horas (o workaround)*.
* **SEV-2 (Alta):** Funcionalidad clave degradada (por ejemplo, ETL con fallos parciales, métricas inconsistentes). *Respuesta: 30 min*, *Resolución: 24 h*.
* **SEV-3 (Media):** Problemas no críticos, errores en una sección del dashboard o avisos de performance. *Respuesta: 4 h*, *Resolución: 3-7 días*.
* **SEV-4 (Baja):** Solicitudes de mejora, documentación, pequeñas correcciones. *Respuesta: 2 días*, *Resolución: 1-4 semanas*.

**Notas:** Ajustar tiempos según contrato y disponibilidad del equipo.

---

## 5. Detección y monitorización (qué vigilar)

**Métricas clave a monitorear:**

* ETL: tasa de ejecuciones exitosas / fallidas, duración del job, último run exitoso timestamp.
* Prefect: flows fallidos, retries, agents offline.
* Supabase/Postgres: conexiones activas, locks, tamaño de base de datos, crecimiento de tablas `raw` y `clean`, queries largas (> X s).
* Streamlit app: errores 5xx, tiempo de respuesta, uso de CPU/RAM en instancia Render.
* Negocio: ingresos diarios, número de órdenes, AOV (alertas si caen > X%).

**Alertas sugeridas:**

* ETL falla 3 veces consecutivas → alert to on-call.
* Error rate API Jumpseller > threshold → alerta.
* DB free disk < 20% → alerta.
* Prefect agent offline por > 10 min → alerta.

**Herramientas recomendadas:** Prometheus/Grafana, Sentry (errores de app), alertas por Slack/Email.

---

## 6. Procedimiento de respuesta a incidentes (runbook general)

1. **Detección:** alertas automáticas o reporte humano.
2. **Triage inicial (L1):** identificar gravedad, asignar SEV y notificar al on-call.
3. **Recolección de evidencia:** timestamps, logs (Render, Prefect logs, Supabase logs), última ejecución del ETL, capturas de pantalla, request IDs si aplican.
4. **Mitigación rápida:** aplicar workaround (reiniciar service, re-ejecutar flow, revert deploy) para reducir impacto.
5. **Investigación:** L2 reproduce y diagnostica causa raíz.
6. **Resolución:** aplicar fix (rollback o patch), validar con tests manuales y automáticos.
7. **Comunicación:** actualizar stakeholders, incident timeline y resolución estimada.
8. **Postmortem:** documentación del incidente, root cause, acciones correctivas, due dates y responsables.

---

## 7. Runbooks (incidentes frecuentes)

> A continuación una serie de runbooks listos para ejecutar. Cada runbook tiene: síntoma, impacto, pasos inmediatos, comprobaciones, comandos, y pasos de escalado.

### 7.1 Runbook: Streamlit no disponible (500 / app caida)

* **Síntomas:** App inaccesible; Render devuelve 5xx; instancia reiniciando.
* **Impacto:** Dashboard inaccesible para usuarios.
* **Pasos inmediatos (L1):**

  1. Revisar **Build logs** y **Instance logs** en Render.
  2. Verificar variables de entorno en panel de Render (SUPABASE_URL, SUPABASE_KEY, PORT, JUMPSELLER_*).
  3. Si el error apareció tras un deploy, revertir al commit previo (GitHub: Revert PR / Reset branch → Render auto-deploy).
  4. Si logs muestran `ModuleNotFoundError` o similar, revisar `requirements.txt` y ejecutar build local en entorno limpio.
* **Comandos útiles (local):**

  ```bash
  # logs locales si corres con docker-compose
  docker-compose logs -f streamlit

  # probar conexión a DB
  psql "postgresql://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE" -c "SELECT 1;"
  ```
* **Escalado:** Si el problema es por recursos (OOM) o dependencias nativas, escalar a L2 (dev) y considerar deploy por Docker.

### 7.2 Runbook: ETL fallo o stuck (Prefect flow fail)

* **Síntomas:** Flow en Prefect falló, jobs en retry o agente desconectado.
* **Impacto:** Datos no actualizados en `clean` → dashboards con datos desactualizados.
* **Pasos inmediatos (L1/L2):**

  1. Revisar la UI de Prefect (o logs del agente) y el ID del run fallido.
  2. Consultar los logs del run para identificar paso con error.
  3. Reintentar el flow si es idempotente: `prefect deployment run <deployment-name>` o usar la opción de re-run en UI.
  4. Si hay excepción por datos corruptos, aislar la entrada (ej. order con formato inesperado) y corregir.
* **Comandos útiles:**

  ```bash
  # listar deployments (Prefect v2)
  prefect deployment ls

  # ejecutar deployment manualmente
  prefect deployment run "project/flow/deployment-name"
  ```
* **Escalado:** Si el agente está offline, verificar el servidor/instancia donde corre el agent y reiniciarlo; si es Prefect Cloud, revisar credenciales y conexión.

### 7.3 Runbook: Error de conexión a Supabase / Postgres

* **Síntomas:** Timeouts en consultas, `could not connect to server`, errores de autenticación.
* **Impacto:** ETL y dashboard no pueden leer/escribir datos.
* **Pasos inmediatos:**

  1. Verificar variables de entorno (`PGHOST`, `PGUSER`, `PGPASSWORD`, `SUPABASE_URL`, `SUPABASE_KEY`).
  2. Intentar conexión directa con `psql` o con un cliente SQL.
  3. Revisar estado del servicio en la consola de Supabase (status) y página de estado del proveedor.
  4. Si hay bloqueo de IP o regla de firewall, validar las IPs de origen.
* **Comandos útiles:**

  ```bash
  psql "postgresql://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE" -c "SELECT now();"
  ```
* **Escalado:** Si el problema es del proveedor (outage), notificar stakeholders y seguir su SLA.

### 7.4 Runbook: Datos inconsistentes entre `raw` y `clean`

* **Síntomas:** KPI show discrepancies (por ejemplo, número de órdenes en `orders_raw` != `orders_clean`).
* **Impacto:** Métricas incorrectas en dashboards.
* **Pasos:**

  1. Identificar última ejecución del ETL y revisar logs del transform step.
  2. Ejecutar queries de conteo por fecha para localizar desde cuándo comenzó la diferencia.
  3. Re-ejecutar la transformación sobre el rango afectado (si es seguro) o aplicar script corrector.
* **Comandos SQL de ayuda:**

  ```sql
  -- comparar conteo por fecha
  SELECT date_trunc('day', created_at) AS dia, count(*) FROM raw.orders_raw GROUP BY 1 ORDER BY 1 DESC LIMIT 30;
  SELECT date_trunc('day', order_date) AS dia, count(*) FROM clean.orders_clean GROUP BY 1 ORDER BY 1 DESC LIMIT 30;
  ```
* **Escalado:** Involucrar L2 si la transformación requiere cambios de código o backfill masivo.

### 7.5 Runbook: Deploy fallido en Render

* **Síntomas:** Build falla o app no levanta tras deploy.
* **Impacto:** App caída en producción.
* **Pasos inmediatos:**

  1. Ver logs de build en Render para identificar el paso fallido.
  2. Si la falla es por instalación de paquetes, reproducir `pip install -r requirements.txt` localmente en un entorno limpio para reproducir.
  3. Revertir a deploy previo (Render auto: revert a commit anterior) si necesario.
  4. Si el build excede límite de disco o timeout, considerar optimizar requirements o migrar a Docker.

---

## 8. Copias de seguridad y restauración (Supabase/Postgres)

**Backups:**

* Planificar backups automatizados (daily) usando `pg_dump` o las herramientas de Supabase.
* Mantener al menos 7 días de backups accesibles.

**Comandos ejemplo:**

```bash
# Dump completo
PGPASSWORD="$PGPASSWORD" pg_dump -h $PGHOST -U $PGUSER -p $PGPORT -Fc -d $PGDATABASE -f backup_$(date +%F).dump

# Restaurar
PGPASSWORD="$PGPASSWORD" pg_restore -h $PGHOST -U $PGUSER -p $PGPORT -d $PGDATABASE -c backup_2025-09-01.dump
```

**Práctica:** Testear restauraciones en un entorno staging regularmente.

---

## 9. Rollback y despliegues seguros

**Rollback rápido:** revertir commit en GitHub → push a `main` → Render auto-deploy al commit previo.

**Blue/Green o Canary:** Para producción crítica, implementar estrategia de despliegue canary o usar dos servicios si Render lo permite.

**Checklist antes del deploy:**

* Tests unitarios/pipelines pasan en CI.
* `requirements.txt` actualizado y probado.
* `env`/secrets configurados en Render.
* Backup de la DB (si hay cambios de schema/DDL).

---

## 10. Postmortem y análisis de incidentes

**Template mínimo:**

* Título del incidente
* Fecha/hora inicio y fin
* Severidad
* Descripción breve
* Timeline (acciones cronológicas con timestamps)
* Root cause
* Impacto cuantitativo (usuarios, pérdida de datos, etc.)
* Acciones correctivas (what, who, due date)
* Lessons learned
* Follow-ups (PRs/Tasks)

Almacenar postmortems en `documentacion/incidentes/` y vincularlos en la página principal de la documentación.

---

## 11. Onboarding para nuevo miembro de soporte

* Repositorios y accesos necesarios (GitHub, Render, Supabase).
* Checklist de acceso: permisos en Supabase, Render, Prefect UI, Slack.
* Entrenamiento: ejecutar localmente el pipeline y desplegar una versión de prueba.
* Leer runbooks y practicar restauración de backup en staging.

---

## 12. Estructura sugerida del directorio `documentacion/`

* `documentacion/soporte-operativo.md`  ← este archivo
* `documentacion/sql/01_create_schemas.sql`
* `documentacion/sql/02_create_public_views.sql`
* `documentacion/scripts/start_streamlit.sh`
* `documentacion/scripts/backup_pg.sh`
* `documentacion/runbooks/streamlit_500.md`
* `documentacion/runbooks/etl_prefect_fail.md`
* `documentacion/incidentes/` ← carpeta con postmortems

---

## 13. Apéndice: comandos y snippets útiles

* Probar conexión Postgres:

  ```bash
  psql "postgresql://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE" -c "SELECT now();"
  ```
* Conteos rápidos SQL:

  ```sql
  SELECT count(*) FROM clean.orders_clean;
  SELECT count(*) FROM raw.orders_raw;
  ```
* Re-ejecutar deployment Prefect (ejemplo):

  ```bash
  prefect deployment run "project/flow/deployment-name"
  ```

---

## 14. Próximos pasos recomendados

1. Crear los archivos de runbook individuales (en `documentacion/runbooks/`).
2. Implementar alertas mínimas (ETL failed, DB disk, Prefect agent offline).
3. Programar backups automáticos y pruebas de restauración.
4. Revisar y aprobar la matriz de contactos y la política de RTO/SLA.

