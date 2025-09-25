
# Definición  **modelo churn**

Un **modelo de churn** es un **modelo supervisado** que, usando información histórica del comportamiento de clientes (ej.: R, F, M y otras señales), **predice la probabilidad de que un cliente deje de comprar durante una ventana futura** $H$ (por ejemplo 90, 156 o 365 días).

* **Output** típico: probabilidad de churn (0–1) o etiqueta binaria (churn = 1 si no compra en la ventana H).
* **Entrada**: features calculadas hasta una fecha de corte $T_0$ (sin mirar el futuro).
* **Target**: definido observando la actividad entre $T_0$ y $T_0+H$.

Importante: por definición práctica, **este modelo se entrena y aplica sobre clientes que ya han comprado al menos una vez** (porque el churn es “dejar de comprar”, no “no comprar nunca”).

---

# Reglas prácticas (qué incluir y qué excluir)

1. **Incluir en el dataset de churn sólo clientes con al menos 1 compra histórica hasta $T_0$** (ever\_purchased = True).
2. **Excluir** (o tratar aparte) usuarios que nunca han comprado (`ever_purchased = False`). A esos se les aplica **un modelo/flujo de activation** (probabilidad de convertir a primera compra).
3. **Filtrar transacciones**: al crear features y labels usa sólo registros de venta válidos (excluir reembolsos, anulaciones, transacciones de prueba).
4. **Crear labels basados en ventanas temporales**: para cada snapshot $T_0$ etiqueta `churn = 1` si el cliente **no** registra compras en (T\_0, T\_0+H]; `churn = 0` si sí compra al menos 1 vez en esa ventana.

---

# Mini-protocolo paso a paso (cómo preparar y usar el modelo)

1. **Elegir H** (tu horizonte). Ej: H = 156 días.
2. **Elegir snapshots** $T_0$ en el pasado (ej.: cada mes) para crear ejemplos múltiples y robustez.
3. **Para cada $T_0$**:

   * Calcular features con datos ≤ $T_0$: recency, frequency (último año/6 meses/90d), monetary, avg\_interpurchase, flags, campañas, etc.
   * Calcular label observando compras en (T\_0, T\_0+H].
   * Mantener sólo clientes con `ever_purchased == True` hasta $T_0$.
4. **Entrenar** modelo (logistic/LGBM/XGBoost etc.) usando validación temporal (forward-chaining).
5. **Evaluar** con PR-AUC, precision\@k, lift y métricas económicas (ROI por campaña).
6. **Desplegar scoring**: periódicamente (ej.: semanal/mensual) recalculas features hasta la fecha actual $T_{\text{now}}$ y aplicas el modelo sobre **clientes con al menos 1 compra** para obtener probabilidades.
7. **Acción**: segmentas por probabilidad / valor esperado (p. ej. top 5% predicted para campaña).
8. **Monitoreo**: comparar churn real vs esperado; recalibrar/reentrenar si hay drift.

---

# ¿Qué hacer con clientes nuevos o sin compras?

* **Clientes sin compras (never\_bought)**: NO se añade con el churn\_model. Crear un flujo separado:

  * *Activation model*: predice probabilidad de primera compra. Features: días desde signup, sesiones, opens emails, campañas recibidas, fuente de adquisición, etc.
  * O bien aplicar reglas (ej.: si `days_since_signup > 30` y no abrió emails → enviar campaña activation).
* **Clientes nuevos que sí tienen 1 compra**: sí se puede puntuar con churn\_model tan pronto como tengan esa 1ª compra (porque ya cumplen la condición “ever\_purchased = True”). Tener en cuenta que la cantidad de historial será pequeña, por lo que las predicciones iniciales tendrán más incertidumbre: considera usar features de corto plazo (count\_last\_30, spent\_last\_30, recency desde esa compra).

---

# Notas y buenas prácticas adicionales


* **Interpretabilidad**: guarda las razones por las que se puntúa alto (ej.: recency alta + baja frecuencia + última compra hace 200 días) para que marketing entienda y personalice mensajes.
* **Alternativa avanzada**: modelos de supervivencia si te interesa predecir *cuándo* churn ocurrirá (tiempo hasta la siguiente compra/abandono).
* **Documenta**: deja claro en tus reportes qué significa “churn” (p. ej. “cliente que no compró en los siguientes 156 días desde el corte”).

---

# Ejemplo de cómo actuar en la práctica (flujo cuando llegan nuevas órdenes)

* Llega nueva data transaccional.
* ETL actualiza tabla de ventas y la tabla `customers` con `last_purchase_date` y `ever_purchased`.
* Ejecutas el pipeline de features (batch) hasta `T_now`.
* Aplicas el modelo **solo** sobre clientes con `ever_purchased == True` (incluye quienes compraron por primera vez ayer).
* Para `never_bought` ejecutas el flujo de activation (o reglas).
* Exportas listas para marketing: recuperación (churn\_high\_prob), nurturing (at-risk), activation (never\_bought\_high\_prob).

