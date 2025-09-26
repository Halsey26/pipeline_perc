# Configuración de Supabase

Documento: **supabase-config.md**

> Esta documentación explica los pasos necesarios para configurar Supabase para el pipeline ETL (esquemas, vistas públicas y variables de entorno). Parte de la `documentacion/` del repo.

---

## 1. Resumen

Supabase será utilizado como la base de datos y capa de API para almacenar los datos crudos (`raw`) y los datos transformados/limpios (`clean`). Para que otras aplicaciones (por ejemplo Streamlit) puedan extraer datos fácilmente desde Supabase, se crearán vistas en el esquema `public` que apuntan a las tablas dentro de `raw` y `clean`.

> **Importante:** Algunos pasos (crear esquemas y vistas públicas) deben ejecutarse **una sola vez** por proyecto.

---

## 2. Requisitos previos

* Cuenta en Supabase.
* Proyecto creado en Supabase (desde la consola de Supabase).
* Acceso al SQL Editor del proyecto (Project -> SQL Editor).
* Credenciales de conexión (user/postgres, password, host, port, dbname) y las variables de entorno de Supabase (`SUPABASE_URL`, `SUPABASE_KEY`).

---

## 3. Variables de entorno (plantilla `.env`)

Crea un archivo `.env` (nunca subir `.env` con secretos al repo). Ejemplo de variables necesarias para la conexión a la base de datos Postgres y al cliente de Supabase:

```
# Postgres connection (si se usa conexión directa)
PGUSER=postgres
PGPASSWORD=tu_password_aqui
PGHOST=tu_host_aqui
PGPORT=5432
PGDATABASE=tu_dbname_aqui

# Supabase (API)
SUPABASE_URL=https://xyzcompany.supabase.co
SUPABASE_KEY=eyJhbGci... (key placeholder)

# (Opcional) Roles / Service keys
# SUPABASE_SERVICE_ROLE_KEY=...  # usar con cuidado (permite privilegios extendidos)
```

> **Nota:** Reemplaza los valores por placeholders y configura las variables reales en el entorno de despliegue (Render, CI/CD, máquinas locales) usando secretos del proveedor.
- Plantilla usada: [`.env` ](https://github.com/Halsey26/pipeline_perc/blob/main/documentacion/variables_config.md)

---

## 4. Crear los esquemas (Ejecutar una sola vez)

En el SQL Editor de Supabase, ejecutar los siguientes comandos para crear los esquemas `raw` y `clean` que contendrán las tablas crudas y las tablas transformadas respectivamente:

```sql
-- Ejecutar una vez
create schema if not exists raw;
create schema if not exists clean;
```

---

## 5. Cargar datos

* **Data raw:** Los datos extraídos de la API de Jumpseller se insertan en tablas bajo el esquema `raw` (por ejemplo: `raw.customers_raw`, `raw.products_raw`, `raw.orders_raw`).
* **Data clean:** Tras la transformación, los datasets procesados se insertan en tablas bajo el esquema `clean` (por ejemplo: `clean.customers_clean`, `clean.products_clean`, `clean.orders_clean`, `clean.orders_products_clean`).

(El detalle de los pipelines ETL y el código de inserción corresponde a la documentación del pipeline; aquí documentamos las partes de Supabase).

---

## 6. Crear vistas públicas (mapear a `public`)

Supabase, por defecto, expone APIs y configuraciones sobre el esquema `public`. Para facilitar que otras aplicaciones lean los datos sin cambiar el esquema interno, creamos vistas en `public` que referencian las tablas de `raw` y `clean`.

**Ejecutar en SQL Editor (ejemplos):**

```sql
-- Views para data_raw
CREATE OR REPLACE VIEW public.customers_raw AS
  SELECT * FROM raw.customers_raw;

CREATE OR REPLACE VIEW public.products_raw AS
  SELECT * FROM raw.products_raw;

CREATE OR REPLACE VIEW public.orders_raw AS
  SELECT * FROM raw.orders_raw;

-- Views para data_clean
CREATE OR REPLACE VIEW public.customers_clean AS
  SELECT * FROM clean.customers_clean;

CREATE OR REPLACE VIEW public.products_clean AS
  SELECT * FROM clean.products_clean;

CREATE OR REPLACE VIEW public.orders_clean AS
  SELECT * FROM clean.orders_clean;

CREATE OR REPLACE VIEW public.orders_products_clean AS
  SELECT * FROM clean.orders_products_clean;
```

> **Recomendación:** Usa `CREATE OR REPLACE VIEW` para poder actualizar la definición si la estructura cambia.

---

## 7. ¿Por qué crear vistas en `public`?

* Supabase suele exponer APIs y permisos por defecto sobre el esquema `public`.
* Crear vistas en `public` permite a herramientas externas (Streamlit, clientes HTTP, etc.) consultar datos sin necesidad de acceder a esquemas no estándar o cambiar permisos de esquema.
* Mantiene la separación lógica entre tablas crudas y procesadas (`raw` y `clean`) y la interfaz estable (`public`).

---

## 8. Permisos y seguridad (puntos clave)

* **No subir credenciales al repo.** Siempre usar secretos del proveedor (Render, GitHub Actions, etc.).
* Si utilizas **anon key** para acceso público, revisa qué endpoints y vistas quedan accesibles. Para operaciones sensibles, utiliza `service_role` key y restringe su uso a entornos de backend seguros.
* Revisa políticas RLS (Row Level Security) si necesitas control fino por usuario/rol.

---

## 9. Ejemplo de conexión (Python) — opción simple

```python
from supabase import create_client
import os

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Ejemplo: leer la vista pública customers_clean
customers = supabase.table('customers_clean').select('*').execute()
print(customers.data)
```

> Dependiendo del cliente que uses (p. ej. `supabase-py` o un driver Postgres), la conexión puede diferir. Si usas conexión directa a Postgres, arma la URL de conexión con los valores de `PGUSER`, `PGPASSWORD`, `PGHOST`, `PGPORT`, `PGDATABASE`.

---

## 10. Buenas prácticas y mantenimiento

* Ejecutar la creación de esquemas y vistas **solo una vez** (o controlarlo con migraciones versionadas si es necesario).
* Versionar en el repo los scripts SQL (por ejemplo en `documentacion/sql/`) pero **sin** credenciales.
* Añadir un `README` corto con pasos para restaurar vistas si se pierden o para reproducir la configuración en otro proyecto.
* Implementar backups periódicos de la base de datos (configuración en Supabase).

---

## 11. Troubleshooting (problemas comunes)

* **Error: tabla no encontrada** — Verifica que la tabla exista en `raw` o `clean` y que el nombre sea correcto.
* **Permisos denegados** — Revisa los roles y la clave usada (anon vs service role). Si usas `anon` algunas vistas o funciones pueden requerir permiso explícito.
* **Datos desactualizados** — Confirma que el pipeline ETL está ejecutándose correctamente y que inserta en el esquema `clean`.


