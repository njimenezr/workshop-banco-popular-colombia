# Módulo Lakeflow SDP (catálogo único `workshop`)

Todo el material del taller **Spark Declarative Pipelines** vive en esta carpeta del mismo repositorio que el workshop Banco Popular de Colombia / Genie Code. Los **ejercicios guiados** (mismo estilo de pasos que Genie Code: descripción, prompt, tips) están en **`exercises/lakeflow-sdp/`** en la raíz del repo; aquí solo está el **SQL del pipeline** y la plantilla de **GRANTS**.

**Cómo encaja con Genie Code:** son **dos estilos de ETL** sobre el mismo catálogo y el **mismo medallón lógico** (`bronze` → `silver` → `gold`):

| Camino | Fuentes (archivos) | Donde materializa |
|--------|--------------------|-------------------|
| **Genie (Data Engineering)** | CSV `/Volumes/workshop/ingest/raw/transacciones_core/transacciones_nuevas.csv` | `workshop.bronze.transacciones_raw`, `workshop.silver.transacciones_clean`, `workshop.gold.fact_transacciones_mensual_genie` (código generado por el participante) |
| **SDP (declarativo)** | JSON `/Volumes/workshop/ingest/raw/` | `workshop.bronze.marketplace_pedidos_raw`, `workshop.silver.marketplace_pedidos_clean`, `workshop.gold.fact_marketplace_pedidos_diario`, y análogo para CDC clientes (`marketplace_clientes_cdc_*` → `dim_marketplace_cliente`) |

El notebook **`generate_workshop_data.py`** (raíz del repo) es la **única preparación**: núcleo en `gold`, semilla marketplace, plantilla Genie, esquemas/volumen `ingest.raw`, JSON y CSV. El pipeline SDP **no** lee tablas `gold` del core; solo `read_files` sobre el volumen.

Diseño basado en el taller original [dbx-Workshop-Declarative-Pipelines](https://github.com/njimenezr/dbx-Workshop-Declarative-Pipelines), adaptado para **un solo catálogo Unity** (`workshop`) y permisos típicos de clientes corporativos.

## Esquemas

| Esquema | Uso |
|---------|-----|
| `workshop.ingest` | Volumen `raw`: JSON marketplace (`orders/`, `status/`, `customers/`) + CSV core Genie (`transacciones_core/`) |
| `workshop.bronze` | Raw Genie + raw SDP (`marketplace_*`) |
| `workshop.silver` | Limpieza Genie + limpieza SDP |
| `workshop.gold` | Núcleo del taller, semillas marketplace, plantilla Genie, hechos/dims **gold** del SDP |

## Orden de trabajo (participante)

1. El facilitador ejecutó **`generate_workshop_data.py`** (Run all) y aplicó **`sql/GRANTS_single_catalog.sql`** al grupo de asistentes.
2. Sube **`sdp-workshop/transformations/`** al Workspace (carpeta `transformations` para el editor multi‑archivo del pipeline).
3. Importa los notebooks de **`exercises/lakeflow-sdp/`** y sigue los pasos del track **Lakeflow SDP** en la app del workshop (o el README de esa carpeta).
4. Crea un **Lakeflow Spark Declarative Pipeline** apuntando a `transformations/orders_pipeline.sql` (y añade `customers_pipeline.sql` en el ejercicio 2).
5. En **Pipeline settings → Configuration** define **`source`** = `/Volumes/workshop/ingest/raw`

## Archivos

| Ruta | Descripción |
|------|-------------|
| `notebooks/00_SETUP_workshop_single_catalog.py` | **Deprecado** — la preparación está en `generate_workshop_data.py` |
| `transformations/orders_pipeline.sql` | Pedidos: bronze → silver → gold |
| `transformations/customers_pipeline.sql` | CDC clientes: bronze → silver → gold |
| `exercises/README.md` | Puntero a **`../../exercises/lakeflow-sdp/`** |
| `utilities/utils.py` | Helpers (igual que upstream) |
| `sql/GRANTS_single_catalog.sql` | Plantilla de permisos para el grupo |

## Varios usuarios en el mismo `workshop`

| Enfoque | Cuándo usarlo | Riesgo |
|---------|----------------|--------|
| **Tablas compartidas** | Demo guiada; un pipeline de referencia | Full refresh simultáneo o escritura en el mismo volumen |
| **Un pipeline por equipo** | Si la cuenta lo permite | Mismos nombres de tabla → conflictos; coordinar o prefijos por equipo |

**Recomendación:** un grupo con `SELECT` sobre `bronze`/`silver`/`gold` y `READ FILES` sobre `ingest.raw`; el facilitador dueño del pipeline.

## Permisos mínimos (resumen)

- Participante: `USE CATALOG workshop`, `USAGE` + `SELECT` en `bronze`, `silver`, `gold`, `READ FILES` (y si aplica `WRITE FILES`) en `ingest.raw`.
- Facilitador: además `CREATE SCHEMA`, `CREATE VOLUME`, permisos de pipeline.

## Troubleshooting rápido

- **`Variable source not found`:** `source` = `/Volumes/workshop/ingest/raw`
- **`Permission denied` en volumen:** `GRANT READ FILES` / `USAGE` sobre el volumen UC
- **Convivencia:** tablas SDP usan prefijos `marketplace_*`, `fact_marketplace_*`, `dim_marketplace_*`; no sustituyen `dim_clientes` ni `fact_transacciones`.
