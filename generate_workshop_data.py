# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Data Generator — Banco Popular de Colombia Genie Code Workshop
# MAGIC
# MAGIC Genera tablas sintéticas de datos bancarios en `workshop.gold` (núcleo del taller + **complemento marketplace** enlazado al módulo SDP).
# MAGIC Incluye defectos de calidad intencionados para el track de Governance.
# MAGIC
# MAGIC **Preparación única (facilitador):** además de `gold`, crea esquemas **`ingest`**, **`bronze`**, **`silver`**, el volumen **`workshop.ingest.raw`**, carpetas de ingesta (JSON marketplace + CSV Genie) y escribe **JSON** + **CSV** para los tracks **Genie Data Engineering** y **Lakeflow SDP** — no hace falta otro notebook de setup.
# MAGIC
# MAGIC **Idempotente** — seguro de re-ejecutar. Usa `CREATE OR REPLACE TABLE`.
# MAGIC
# MAGIC **Catálogo `workshop`:** si tu organización ya lo creó y no tienes `CREATE CATALOG`, pide al admin que otorgue `USE CATALOG` y `CREATE SCHEMA` / `CREATE TABLE` en `workshop`, o comenta la celda `CREATE CATALOG` y ejecuta solo a partir de `CREATE SCHEMA`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

CATALOG = "workshop"
SCHEMA = "gold"

# Regiones sintéticas (hub territorial) — taller Banco Popular de Colombia; datos 100% ficticios
COUNTRIES = {
    "GY": {
        "name": "Costa — Guayas",
        "customers": 500,
        "branches": 28,
        "cities": ["Guayaquil", "Durán", "Samborondón", "Milagro", "Daule", "Playas"],
        "lat_range": (-3.0, -1.8),
        "lon_range": (-80.5, -79.5),
        "base_portfolio": 180_000_000,
    },
    "PI": {
        "name": "Sierra — Pichincha",
        "customers": 380,
        "branches": 22,
        "cities": ["Quito", "Cayambe", "Rumiñahui", "Mejía"],
        "lat_range": (-0.5, 0.4),
        "lon_range": (-78.7, -78.2),
        "base_portfolio": 220_000_000,
    },
    "AZ": {
        "name": "Austro — Azuay",
        "customers": 320,
        "branches": 18,
        "cities": ["Cuenca", "Gualaceo", "Paute", "Santa Isabel"],
        "lat_range": (-3.2, -2.6),
        "lon_range": (-79.3, -78.6),
        "base_portfolio": 140_000_000,
    },
    "MN": {
        "name": "Costa — Manabí",
        "customers": 270,
        "branches": 15,
        "cities": ["Manta", "Portoviejo", "Bahía de Caráquez", "Chone"],
        "lat_range": (-1.3, -0.4),
        "lon_range": (-80.9, -79.6),
        "base_portfolio": 260_000_000,
    },
    "OR": {
        "name": "Costa — El Oro",
        "customers": 250,
        "branches": 14,
        "cities": ["Machala", "Pasaje", "Santa Rosa", "Huaquillas"],
        "lat_range": (-3.8, -3.2),
        "lon_range": (-80.2, -79.6),
        "base_portfolio": 120_000_000,
    },
    "ES": {
        "name": "Costa — Esmeraldas",
        "customers": 220,
        "branches": 13,
        "cities": ["Esmeraldas", "Atacames", "Muisne", "Rioverde"],
        "lat_range": (0.6, 1.2),
        "lon_range": (-79.7, -78.8),
        "base_portfolio": 100_000_000,
    },
    "LO": {
        "name": "Sur — Loja",
        "customers": 170,
        "branches": 10,
        "cities": ["Loja", "Catamayo", "Macará", "Zapotillo"],
        "lat_range": (-4.4, -3.7),
        "lon_range": (-80.3, -79.2),
        "base_portfolio": 90_000_000,
    },
    "SD": {
        "name": "Sierra — Santo Domingo",
        "customers": 290,
        "branches": 16,
        "cities": ["Santo Domingo", "La Concordia", "Valle Hermoso"],
        "lat_range": (-0.4, 0.2),
        "lon_range": (-79.4, -78.8),
        "base_portfolio": 195_000_000,
    },
}

SEGMENTS = ["Retail", "PYME", "Corporativo", "Premium"]
SEGMENT_WEIGHTS = [0.55, 0.25, 0.10, 0.10]
RISK_PROFILES = ["A", "B", "C", "D", "E"]
RISK_WEIGHTS = [0.30, 0.35, 0.20, 0.10, 0.05]
KYC_STATUSES = ["Vigente", "Vencido", "Pendiente"]
KYC_WEIGHTS = [0.80, 0.12, 0.08]
PRODUCT_TYPES = ["Consumo", "Hipoteca", "Vehiculo", "Comercial", "Tarjeta"]
PRODUCT_WEIGHTS = [0.38, 0.22, 0.18, 0.14, 0.08]
TRANSACTION_TYPES = ["Débito", "Crédito", "Transferencia", "Pago"]
CHANNELS = ["Sucursal", "App", "ATM", "Web", "Corresponsal"]
CHANNEL_WEIGHTS = [0.25, 0.35, 0.18, 0.15, 0.07]
PRODUCTS_TX = ["Cuenta_Ahorros", "Cuenta_Corriente", "Tarjeta_Credito", "Deposito_Plazo"]
BRANCH_TYPES = ["Sucursal", "Agencia", "Corresponsal", "Digital"]
DPD_BUCKETS = ["Al_Dia", "1-30", "31-60", "61-90", "Mayor_90"]

total_customers = sum(c["customers"] for c in COUNTRIES.values())
total_branches = sum(c["branches"] for c in COUNTRIES.values())
print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Total clientes: {total_customers:,} | Total sucursales: {total_branches}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Catálogo y Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ {CATALOG}.{SCHEMA} listo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparación Unity Catalog (todos los tracks)
# MAGIC
# MAGIC **Un solo notebook de preparación** para Genie Code y Lakeflow SDP: crea esquemas **`ingest`**, **`bronze`**, **`silver`**, **`gold`**, el volumen **`workshop.ingest.raw`** y las carpetas bajo `/Volumes/workshop/ingest/raw/` (**`orders/`**, **`status/`**, **`customers/`** para JSON del pipeline; **`transacciones_core/`** para el CSV del track Data Engineering).
# MAGIC
# MAGIC Las celdas posteriores rellenan tablas `gold`, JSON alineados con `fact_pedidos_marketplace` y el CSV desde `fact_transacciones`.

# COMMAND ----------

for _prep_schema in ("ingest", "bronze", "silver", SCHEMA):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_prep_schema}")
    print(f"✅ Esquema {CATALOG}.{_prep_schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ingest.raw")
print(f"✅ Volumen {CATALOG}.ingest.raw")

_prep_ingest_root = f"/Volumes/{CATALOG}/ingest/raw"
for _prep_sub in ("orders", "status", "customers", "transacciones_core"):
    dbutils.fs.mkdirs(f"{_prep_ingest_root}/{_prep_sub}")
print(f"✅ Carpetas en {_prep_ingest_root}/ (ingesta Genie + SDP)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 1: dim_clientes

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date, timedelta

np.random.seed(42)

customer_rows = []
customer_id_counter = 0

for cc, info in COUNTRIES.items():
    for i in range(info["customers"]):
        customer_id_counter += 1
        cid = f"BPC-{cc}-CLI-{customer_id_counter:06d}"
        city = np.random.choice(info["cities"])
        segment = np.random.choice(SEGMENTS, p=SEGMENT_WEIGHTS)
        risk = np.random.choice(RISK_PROFILES, p=RISK_WEIGHTS)
        kyc = np.random.choice(KYC_STATUSES, p=KYC_WEIGHTS)

        # Credit score correlated with risk profile
        score_ranges = {"A": (720, 850), "B": (620, 720), "C": (520, 620), "D": (420, 520), "E": (300, 420)}
        credit_score = int(np.random.uniform(*score_ranges[risk]))

        acquisition_date = date(2010, 1, 1) + timedelta(days=int(np.random.uniform(0, 365 * 14)))

        # Relationship managers per region
        rm_names = [
            f"Ana García ({cc})", f"Carlos Méndez ({cc})", f"María López ({cc})",
            f"Jorge Solís ({cc})", f"Patricia Vega ({cc})", f"Roberto Acuña ({cc})"
        ]
        rm = np.random.choice(rm_names)

        customer_rows.append({
            "customer_id": cid,
            "customer_name": f"Cliente {cc} {i+1:04d}",
            "segment": segment,
            "country_code": cc,
            "country_name": info["name"],
            "city": city,
            "credit_score": credit_score,
            "risk_profile": risk,
            "kyc_status": kyc,
            "acquisition_date": acquisition_date,
            "relationship_manager": rm,
        })

df_customers = pd.DataFrame(customer_rows)

# ── Inyectar defectos DQ (contexto bancario) ──
# 10 filas: NULL country_code
null_cc_idx = np.random.choice(len(df_customers), 10, replace=False)
df_customers.loc[null_cc_idx, "country_code"] = None

# 6 filas: fecha de adquisición futura
future_idx = np.random.choice(len(df_customers), 6, replace=False)
df_customers.loc[future_idx, "acquisition_date"] = date(2027, 3, 15)

# 5 filas: customer_id duplicado
dup_idx = np.random.choice(len(df_customers), 5, replace=False)
for idx in dup_idx:
    source_idx = np.random.choice([i for i in range(len(df_customers)) if i != idx])
    df_customers.loc[idx, "customer_id"] = df_customers.loc[source_idx, "customer_id"]

# 15 filas: segment en minúsculas (inconsistencia de capitalización)
case_idx = np.random.choice(len(df_customers), 15, replace=False)
df_customers.loc[case_idx, "segment"] = df_customers.loc[case_idx, "segment"].str.lower()

# 18 filas: credit_score fuera de rango (<300 o >850)
score_idx = np.random.choice(len(df_customers), 18, replace=False)
df_customers.loc[score_idx[:9], "credit_score"] = 150   # imposible — por debajo del mínimo
df_customers.loc[score_idx[9:], "credit_score"] = 950   # imposible — por encima del máximo

print(f"dim_clientes: {len(df_customers)} filas")
print(f"  Defectos DQ: 10 null country, 6 fechas futuras, 5 IDs dup, 15 segment minúsculas, 18 scores inválidos")

sdf = spark.createDataFrame(df_customers)
sdf.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_clientes")
print(f"✅ {CATALOG}.{SCHEMA}.dim_clientes escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 2: dim_sucursales

# COMMAND ----------

branch_rows = []
branch_counter = 0
valid_branch_ids = []

for cc, info in COUNTRIES.items():
    for i in range(info["branches"]):
        branch_counter += 1
        bid = f"SUC-{cc}-{branch_counter:04d}"
        city = np.random.choice(info["cities"])
        btype = np.random.choice(BRANCH_TYPES, p=[0.40, 0.30, 0.20, 0.10])
        lat = np.random.uniform(*info["lat_range"])
        lon = np.random.uniform(*info["lon_range"])
        region = np.random.choice(["Norte", "Centro", "Sur", "Capital"])
        valid_branch_ids.append(bid)

        branch_rows.append({
            "branch_id": bid,
            "branch_name": f"Banco Popular de Colombia {city} #{i+1}",
            "city": city,
            "country_code": cc,
            "country_name": info["name"],
            "branch_type": btype,
            "region": region,
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
        })

df_branches = pd.DataFrame(branch_rows)

# ── Inyectar defectos DQ ──
# 8 filas: NULL country_code
null_cc_b = np.random.choice(len(df_branches), 8, replace=False)
df_branches.loc[null_cc_b, "country_code"] = None

# 3 filas: branch_id duplicado
dup_b = np.random.choice(len(df_branches), 3, replace=False)
for idx in dup_b:
    src = np.random.choice([i for i in range(len(df_branches)) if i != idx])
    df_branches.loc[idx, "branch_id"] = df_branches.loc[src, "branch_id"]

# 10 filas: branch_type en minúsculas
case_b = np.random.choice(len(df_branches), 10, replace=False)
df_branches.loc[case_b, "branch_type"] = df_branches.loc[case_b, "branch_type"].str.lower()

print(f"dim_sucursales: {len(df_branches)} filas")
print(f"  Defectos DQ: 8 null country, 3 IDs dup, 10 branch_type minúsculas")

sdf_b = spark.createDataFrame(df_branches)
sdf_b.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_sucursales")
print(f"✅ {CATALOG}.{SCHEMA}.dim_sucursales escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 3: fact_transacciones

# COMMAND ----------

valid_customer_ids = df_customers["customer_id"].dropna().unique().tolist()
date_range = pd.date_range("2025-01-01", "2026-04-30", freq="D")
tx_rows = []

# Generar transacciones para los primeros 600 clientes (muestra representativa)
sample_customers = valid_customer_ids[:600]

for cid in sample_customers:
    # Obtener datos del cliente
    cust = df_customers[df_customers["customer_id"] == cid].iloc[0]
    cc = cust["country_code"] if pd.notna(cust["country_code"]) else "GY"
    segment = cust["segment"]

    # Frecuencia según segmento
    freq_per_month = {"Retail": 8, "PYME": 15, "Corporativo": 25, "Premium": 12}.get(segment, 8)

    # Monto base según segmento y región
    country_mult = {"GY": 1.0, "PI": 1.8, "AZ": 0.9, "MN": 2.2, "OR": 1.0, "ES": 0.95, "LO": 0.85, "SD": 1.3}
    base_amount = {"Retail": 350, "PYME": 5000, "Corporativo": 50000, "Premium": 8000}.get(segment, 350)
    base_amount *= country_mult.get(cc, 1.0)

    branch_pool = [b for b in valid_branch_ids if cc in b]
    if not branch_pool:
        branch_pool = valid_branch_ids[:5]

    for d in date_range:
        # Probabilidad diaria de transacción
        if np.random.random() < freq_per_month / 30:
            n_tx = np.random.randint(1, 4)
            for _ in range(n_tx):
                amount = max(1.0, np.random.lognormal(np.log(base_amount), 0.6))
                tx_type = np.random.choice(TRANSACTION_TYPES)
                channel = np.random.choice(CHANNELS, p=CHANNEL_WEIGHTS)
                product = np.random.choice(PRODUCTS_TX)
                status = np.random.choice(["Aprobada", "Rechazada", "Pendiente"], p=[0.88, 0.08, 0.04])
                branch = np.random.choice(branch_pool)

                tx_rows.append({
                    "transaction_id": f"TX-{cc}-{len(tx_rows)+1:08d}",
                    "customer_id": cid,
                    "branch_id": branch,
                    "transaction_date": d.date(),
                    "amount": round(amount, 2),
                    "transaction_type": tx_type,
                    "channel": channel,
                    "product": product,
                    "currency": "USD",
                    "status": status,
                })

df_tx = pd.DataFrame(tx_rows)

# ── Inyectar defectos DQ ──
# 150 filas: amount = 0 con status 'Aprobada'
zero_amt_idx = np.random.choice(len(df_tx), 150, replace=False)
df_tx.loc[zero_amt_idx, "amount"] = 0.0
df_tx.loc[zero_amt_idx, "status"] = "Aprobada"

# 30 filas: NULL transaction_date
null_date_idx = np.random.choice(len(df_tx), 30, replace=False)
df_tx.loc[null_date_idx, "transaction_date"] = None

# 60 filas: customer_id huérfano (no existe en dim_clientes — integridad referencial rota)
orphan_cids = [f"BPC-XX-CLI-{900000+i:06d}" for i in range(60)]
orphan_idx = np.random.choice(len(df_tx), 60, replace=False)
for i, idx in enumerate(orphan_idx):
    df_tx.loc[idx, "customer_id"] = orphan_cids[i]

# 25 filas: amount negativo en transacciones no-reversión
neg_idx = np.random.choice(len(df_tx), 25, replace=False)
df_tx.loc[neg_idx, "amount"] = -abs(df_tx.loc[neg_idx, "amount"])

print(f"fact_transacciones: {len(df_tx):,} filas")
print(f"  Defectos DQ: 150 monto cero aprobado, 30 null fecha, 60 clientes huérfanos, 25 montos negativos")

sdf_tx = spark.createDataFrame(df_tx)
sdf_tx.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_transacciones")
print(f"✅ {CATALOG}.{SCHEMA}.fact_transacciones escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 4: fact_cartera_creditos

# COMMAND ----------

loan_rows = []
loan_counter = 0

for cc, info in COUNTRIES.items():
    country_customers = df_customers[df_customers["country_code"] == cc]["customer_id"].tolist()
    base_port = info["base_portfolio"]
    n_loans = int(base_port / 25_000)   # ~25K USD promedio por crédito

    for i in range(n_loans):
        loan_counter += 1
        product = np.random.choice(PRODUCT_TYPES, p=PRODUCT_WEIGHTS)
        loan_id = f"BPC-{cc}-{product[:3].upper()}-{loan_counter:06d}"

        cid = np.random.choice(country_customers) if country_customers else f"BPC-{cc}-CLI-000001"

        disb_date = date(2020, 1, 1) + timedelta(days=int(np.random.uniform(0, 365 * 5)))

        # Plazo según producto
        term_months = {
            "Consumo": np.random.choice([12, 24, 36, 48, 60]),
            "Hipoteca": np.random.choice([120, 180, 240, 300]),
            "Vehiculo": np.random.choice([24, 36, 48, 60]),
            "Comercial": np.random.choice([12, 24, 36]),
            "Tarjeta": 12,
        }[product]

        maturity_date = disb_date + timedelta(days=term_months * 30)

        # Monto original según producto y región
        amount_ranges = {
            "Consumo": (2000, 25000),
            "Hipoteca": (40000, 250000),
            "Vehiculo": (8000, 45000),
            "Comercial": (10000, 500000),
            "Tarjeta": (500, 10000),
        }
        original_amount = round(np.random.uniform(*amount_ranges[product]), 2)

        # Saldo pendiente (entre 0 y original)
        elapsed_pct = min(1.0, (date(2026, 4, 30) - disb_date).days / (term_months * 30))
        balance_pct = max(0, 1 - elapsed_pct * np.random.uniform(0.8, 1.2))
        outstanding = round(original_amount * balance_pct, 2)
        monthly_pmt = round(original_amount / term_months * np.random.uniform(1.0, 1.05), 2)

        # Mora — distribución realista por perfil de riesgo
        cust_data = df_customers[df_customers["customer_id"] == cid]
        if len(cust_data) > 0:
            risk = cust_data.iloc[0]["risk_profile"]
        else:
            risk = "B"

        dpd_probs = {
            "A": [0.90, 0.06, 0.025, 0.010, 0.005],
            "B": [0.82, 0.10, 0.045, 0.025, 0.010],
            "C": [0.65, 0.16, 0.090, 0.065, 0.035],
            "D": [0.42, 0.22, 0.160, 0.120, 0.080],
            "E": [0.20, 0.18, 0.160, 0.180, 0.280],
        }
        dpd_bucket = np.random.choice(DPD_BUCKETS, p=dpd_probs.get(risk, dpd_probs["B"]))

        dpd_ranges = {"Al_Dia": (0, 0), "1-30": (1, 30), "31-60": (31, 60), "61-90": (61, 90), "Mayor_90": (91, 730)}
        days_past_due = int(np.random.uniform(*dpd_ranges[dpd_bucket]))

        status_map = {"Al_Dia": "Vigente", "1-30": "Vigente", "31-60": "Vencido", "61-90": "Vencido", "Mayor_90": "Castigado"}
        status = status_map[dpd_bucket]

        # Tasa de interés según producto (mercado regional)
        rate_ranges = {
            "Consumo": (0.14, 0.32),
            "Hipoteca": (0.08, 0.15),
            "Vehiculo": (0.10, 0.20),
            "Comercial": (0.10, 0.25),
            "Tarjeta": (0.24, 0.48),
        }
        interest_rate = round(np.random.uniform(*rate_ranges[product]), 4)

        loan_rows.append({
            "loan_id": loan_id,
            "customer_id": cid,
            "product_type": product,
            "disbursement_date": disb_date,
            "maturity_date": maturity_date,
            "original_amount": original_amount,
            "outstanding_balance": outstanding,
            "monthly_payment": monthly_pmt,
            "days_past_due": days_past_due,
            "dpd_bucket": dpd_bucket,
            "status": status,
            "interest_rate": interest_rate,
            "country_code": cc,
        })

df_loans = pd.DataFrame(loan_rows)

# ── Inyectar defectos DQ ──
# 8 filas: days_past_due negativo (imposible)
neg_dpd_idx = np.random.choice(len(df_loans), 8, replace=False)
df_loans.loc[neg_dpd_idx, "days_past_due"] = -np.random.randint(1, 30, 8)

# 4 filas: maturity_date antes de disbursement_date
bad_maturity_idx = np.random.choice(len(df_loans), 4, replace=False)
df_loans.loc[bad_maturity_idx, "maturity_date"] = df_loans.loc[bad_maturity_idx, "disbursement_date"] - timedelta(days=180)

# 10 filas: outstanding_balance > original_amount * 1.2 (incapitalizaciones mal registradas)
over_balance_idx = np.random.choice(len(df_loans), 10, replace=False)
df_loans.loc[over_balance_idx, "outstanding_balance"] = df_loans.loc[over_balance_idx, "original_amount"] * np.random.uniform(1.25, 1.80, 10)

# 5 filas: NULL interest_rate
null_rate_idx = np.random.choice(len(df_loans), 5, replace=False)
df_loans.loc[null_rate_idx, "interest_rate"] = None

# 12 filas: dpd_bucket inconsistente con days_past_due
inconsistent_idx = np.random.choice(len(df_loans), 12, replace=False)
for idx in inconsistent_idx:
    correct_bucket = df_loans.loc[idx, "dpd_bucket"]
    wrong_options = [b for b in DPD_BUCKETS if b != correct_bucket]
    df_loans.loc[idx, "dpd_bucket"] = np.random.choice(wrong_options)

print(f"fact_cartera_creditos: {len(df_loans):,} filas")
print(f"  Defectos DQ: 8 DPD negativos, 4 madurez < desembolso, 10 saldo > original, 5 null tasa, 12 bucket inconsistente")

sdf_loans = spark.createDataFrame(df_loans)
sdf_loans.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_cartera_creditos")
print(f"✅ {CATALOG}.{SCHEMA}.fact_cartera_creditos escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 5: fact_kpis_diarios

# COMMAND ----------

kpi_rows = []
kpi_date_range = pd.date_range("2024-01-01", "2026-04-30", freq="D")

for cc, info in COUNTRIES.items():
    base_port = info["base_portfolio"]

    for d in kpi_date_range:
        years_from_start = (d - pd.Timestamp("2024-01-01")).days / 365
        growth = 1 + 0.04 * years_from_start

        month_mult = {1: 0.95, 2: 0.90, 3: 0.97, 4: 1.00, 5: 1.02, 6: 1.00,
                      7: 1.03, 8: 1.01, 9: 0.98, 10: 1.00, 11: 1.04, 12: 1.08}[d.month]

        noise = np.random.normal(1.0, 0.03)
        total_port = round(base_port * growth * month_mult * noise, 2)

        npl_base = {"GY": 0.042, "PI": 0.028, "AZ": 0.051, "MN": 0.022, "OR": 0.055,
                    "ES": 0.048, "LO": 0.062, "SD": 0.035}.get(cc, 0.04)
        npl_ratio = round(npl_base + np.random.normal(0, 0.003), 4)
        npl_ratio = max(0.005, npl_ratio)

        new_disb = round(total_port * np.random.uniform(0.008, 0.025) * month_mult, 2)
        collections = round(total_port * np.random.uniform(0.005, 0.015), 2)
        provision_exp = round(total_port * npl_ratio * np.random.uniform(0.15, 0.30), 2)

        active_cust = int(info["customers"] * np.random.uniform(0.75, 0.95))
        new_cust = int(np.random.uniform(0, 8))

        yoy_growth = None
        if d.year >= 2025:
            yoy_growth = round(np.random.normal(0.04, 0.02), 4)

        kpi_rows.append({
            "kpi_date": d.date(),
            "country_code": cc,
            "country_name": info["name"],
            "total_portfolio": total_port,
            "npl_ratio": npl_ratio,
            "new_disbursements": new_disb,
            "collections": collections,
            "provision_expense": provision_exp,
            "active_customers": active_cust,
            "new_customers": new_cust,
            "yoy_portfolio_growth": yoy_growth,
        })

df_kpis = pd.DataFrame(kpi_rows)

# ── Inyectar defectos DQ ──
invalid_npl_idx = np.random.choice(len(df_kpis), 8, replace=False)
df_kpis.loc[invalid_npl_idx, "npl_ratio"] = np.random.uniform(1.5, 3.0, 8)

zero_port_idx = np.random.choice(len(df_kpis), 5, replace=False)
df_kpis.loc[zero_port_idx, "total_portfolio"] = 0.0

sentinel_idx = df_kpis[df_kpis["yoy_portfolio_growth"].notna()].sample(3).index
df_kpis.loc[sentinel_idx, "yoy_portfolio_growth"] = 999.99

unknown_idx = np.random.choice(len(df_kpis), 6, replace=False)
df_kpis.loc[unknown_idx, "country_code"] = "DESCONOCIDO"

print(f"fact_kpis_diarios: {len(df_kpis):,} filas")
print(f"  Defectos DQ: 8 NPL>1.0, 5 portfolio=0 con desembolsos, 3 centinelas, 6 país DESCONOCIDO")

sdf_kpis = spark.createDataFrame(df_kpis)
sdf_kpis.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_kpis_diarios")
print(f"✅ {CATALOG}.{SCHEMA}.fact_kpis_diarios escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 6: dim_categoria_pedido_digital
# MAGIC
# MAGIC Dimensión pequeña de **canasta / marketplace digital** (comercio electrónico sobre la misma base de clientes y sucursales). Sirve para análisis complementarios a `fact_transacciones` y para enlazar con los pedidos que alimentan el landing SDP (JSON).

# COMMAND ----------

MKT_CATEGORIES = [
    ("CAT-DIG-01", "Canasta básica y pagos servicios", "Bajo"),
    ("CAT-DIG-02", "Supermercado y conveniencia", "Bajo"),
    ("CAT-DIG-03", "Farmacia y salud", "Medio"),
    ("CAT-DIG-04", "Electrónica y electrodomésticos", "Medio"),
    ("CAT-DIG-05", "Moda y calzado", "Bajo"),
    ("CAT-DIG-06", "Agro y ferretería", "Bajo"),
    ("CAT-DIG-07", "Entradas y entretenimiento", "Bajo"),
    ("CAT-DIG-08", "Marketplace terceros (comisión)", "Alto"),
]

df_mkt_cat = pd.DataFrame(
    [{"category_id": a, "category_name": b, "aml_risk_tier": c} for a, b, c in MKT_CATEGORIES]
)
spark.createDataFrame(df_mkt_cat).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_categoria_pedido_digital")
print(f"✅ {CATALOG}.{SCHEMA}.dim_categoria_pedido_digital ({len(df_mkt_cat)} filas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 7: fact_pedidos_marketplace
# MAGIC
# MAGIC Hechos de **pedidos digitales** (`order_id` estilo `ORD01000` …) con `customer_id` y `branch_id` **reales** de `dim_clientes` / `dim_sucursales`. No sustituye al core contable: `source_system = MARKETPLACE_DIGITAL`. Los mismos `order_id` / `customer_id` se escriben en el JSON de `ingest` para que el taller SDP siga siendo **independiente** (solo lee archivos) pero **complementario** en SQL (`JOIN` opcional con `gold`).

# COMMAND ----------

np.random.seed(44)
pool_cust = df_customers.loc[df_customers["country_code"].notna(), "customer_id"].tolist()
if len(pool_cust) < 50:
    pool_cust = df_customers["customer_id"].dropna().unique().tolist()

cat_ids = [c[0] for c in MKT_CATEGORIES]
n_mkt_orders = 174
mkt_rows = []
base_ts0 = pd.Timestamp("2025-01-01")

for i in range(n_mkt_orders):
    oid = f"ORD{1000 + i:05d}"
    cid = np.random.choice(pool_cust)
    crow = df_customers.loc[df_customers["customer_id"] == cid].iloc[0]
    cc = crow["country_code"] if pd.notna(crow["country_code"]) else "GY"
    bpool = df_branches.loc[df_branches["country_code"] == cc, "branch_id"].tolist()
    if not bpool:
        bpool = df_branches["branch_id"].tolist()
    bid = np.random.choice(bpool)
    cat_id = np.random.choice(cat_ids)
    order_ts = base_ts0 + pd.Timedelta(days=int(np.random.randint(0, 480)), hours=int(np.random.randint(8, 20)))
    amt = round(float(max(5.0, np.random.lognormal(3.6, 0.75))), 2)
    ful = np.random.choice(
        ["placed", "preparing", "on the way", "delivered", "canceled"],
        p=[0.05, 0.08, 0.12, 0.68, 0.07],
    )
    mkt_rows.append(
        {
            "order_id": oid,
            "customer_id": cid,
            "branch_id": bid,
            "category_id": cat_id,
            "order_timestamp": order_ts,
            "gross_amount": amt,
            "currency": "USD",
            "fulfillment_status": ful,
            "source_system": "MARKETPLACE_DIGITAL",
        }
    )

df_mkt = pd.DataFrame(mkt_rows)
spark.createDataFrame(df_mkt).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_pedidos_marketplace")
print(f"✅ {CATALOG}.{SCHEMA}.fact_pedidos_marketplace ({len(df_mkt):,} filas) — FKs a dim_clientes, dim_sucursales, dim_categoria_pedido_digital")

# Tabla gold del camino Genie (medallión CSV): vacía hasta que el participante ejecute el paso 2 del track DE
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

_ftg = f"{CATALOG}.{SCHEMA}.fact_transacciones_mensual_genie"
_ftg_schema = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("year_month", StringType(), True),
        StructField("total_transactions", LongType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("avg_ticket", DoubleType(), True),
        StructField("top_channel", StringType(), True),
    ]
)
if not spark.catalog.tableExists(_ftg):
    spark.createDataFrame([], _ftg_schema).write.saveAsTable(_ftg)
    print(f"✅ {_ftg} creada (0 filas) — relleno por el camino Genie Code (paso 2)")
else:
    print(f"✓ {_ftg} ya existe — no se sobrescribe (puede contener datos del taller)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing SDP (JSON alineado — taller SDP sigue independiente)
# MAGIC
# MAGIC Crea `workshop.ingest.raw` si hace falta y escribe `orders/`, `status/`, `customers/` con **mismos** `customer_id` (BPC-…) y `order_id` (ORD…) que `fact_pedidos_marketplace`. El pipeline SDP **solo** lee archivos; materializa el medallón en **`workshop.bronze`**, **`workshop.silver`** y **`workshop.gold`** (`marketplace_*`, `fact_marketplace_*`, `dim_marketplace_*` — ver `sdp-workshop/transformations/*.sql`).

# COMMAND ----------

try:
    SDP_RAW = f"/Volumes/{CATALOG}/ingest/raw"
    for sub in ("orders", "status", "customers"):
        dbutils.fs.mkdirs(f"{SDP_RAW}/{sub}")

    import json as _json

    orders_json = []
    for _, r in df_mkt.iterrows():
        ts = r["order_timestamp"]
        if hasattr(ts, "isoformat"):
            ts_iso = pd.Timestamp(ts).isoformat()
        else:
            ts_iso = str(ts)
        orders_json.append(
            {
                "order_id": r["order_id"],
                "order_timestamp": ts_iso,
                "customer_id": r["customer_id"],
                "notifications": {
                    "email": bool(np.random.randint(0, 2)),
                    "sms": bool(np.random.randint(0, 2)),
                },
            }
        )
    dbutils.fs.put(f"{SDP_RAW}/orders/00.json", "\n".join(_json.dumps(o) for o in orders_json), overwrite=True)

    statuses = ["placed", "preparing", "on the way", "delivered", "canceled"]
    order_ids_arr = df_mkt["order_id"].tolist()
    base_unix = pd.Timestamp("2025-01-01").timestamp()
    status_lines = []
    for i in range(536):
        oid = np.random.choice(order_ids_arr)
        st = np.random.choice(statuses)
        status_lines.append(
            _json.dumps(
                {
                    "order_id": oid,
                    "order_status": st,
                    "status_timestamp": base_unix + (i * 3600),
                }
            )
        )
    dbutils.fs.put(f"{SDP_RAW}/status/00.json", "\n".join(status_lines), overwrite=True)

    # CDC clientes: solo clientes que aparecen en pedidos marketplace + eventos INSERT/UPDATE/DELETE
    mkt_customers = df_mkt["customer_id"].unique().tolist()
    np.random.shuffle(mkt_customers)
    n_profile = min(24, len(mkt_customers))
    profile_ids = mkt_customers[:n_profile]
    cdc_events = []
    base_cdc_ts = pd.Timestamp("2025-01-01").timestamp()

    def _safe_email(idx: int) -> str:
        return f"bgymkt{idx:04d}@digital.bgy.ec"

    for j, cust_id in enumerate(profile_ids):
        row = df_customers.loc[df_customers["customer_id"] == cust_id].iloc[0]
        city = str(row["city"]) if pd.notna(row.get("city")) else "Guayaquil"
        cc = str(row["country_code"]) if pd.notna(row.get("country_code")) else "GY"
        cdc_events.append(
            {
                "customer_id": cust_id,
                "name": str(row["customer_name"])[:120],
                "email": _safe_email(j + 1),
                "address": f"Av. Digital {100 + j}",
                "city": city,
                "state": cc,
                "zip_code": f"{10000 + (j * 97) % 90000:05d}",
                "operation": "INSERT",
                "timestamp": base_cdc_ts + (j + 1) * 1000,
            }
        )

    _upd_idx = [0, 4, 8, 12, 16]
    for upd_j, idx in enumerate(_upd_idx):
        if idx >= len(profile_ids):
            continue
        cust_id = profile_ids[idx]
        cdc_events.append(
            {
                "customer_id": cust_id,
                "name": str(df_customers.loc[df_customers["customer_id"] == cust_id, "customer_name"].iloc[0])[:120],
                "email": _safe_email(500 + upd_j),
                "address": f"{200 + upd_j * 11} Calle Río",
                "city": "Quito",
                "state": "PI",
                "zip_code": f"{17000 + upd_j:05d}",
                "operation": "UPDATE",
                "timestamp": base_cdc_ts + 40_000 + upd_j * 200,
            }
        )

    _del_idx = [2, 10]
    for del_j, idx in enumerate(_del_idx):
        if idx >= len(profile_ids):
            continue
        cust_id = profile_ids[idx]
        cdc_events.append(
            {
                "customer_id": cust_id,
                "operation": "DELETE",
                "timestamp": base_cdc_ts + 70_000 + del_j * 150,
            }
        )

    cdc_events.sort(key=lambda x: x["timestamp"])
    dbutils.fs.put(f"{SDP_RAW}/customers/00.json", "\n".join(_json.dumps(c) for c in cdc_events), overwrite=True)

    print(f"✅ SDP landing JSON alineado con gold: {SDP_RAW}/ (orders/status/customers)")
except Exception as e:
    print(f"⚠️ No se pudo crear ingest o escribir JSON SDP: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV de “llegada del core” + esquemas medallión (Genie Data Engineering)
# MAGIC
# MAGIC El track **Data Engineering** (Genie Code) simula un archivo diario del core bancario. Esas filas salen de **`gold.fact_transacciones`** (mismo universo de datos que el resto del workshop).
# MAGIC
# MAGIC Se crean también `workshop.bronze` y `workshop.silver` vacíos para que el código que genera Genie en el paso “medallión” pueda escribir sin error de esquema inexistente.
# MAGIC
# MAGIC **Ruta del CSV (Genie):** mismo volumen que el landing SDP — **`/Volumes/workshop/ingest/raw/transacciones_core/transacciones_nuevas.csv`** (carpeta **`transacciones_core`**, separada de `orders/`, `status/`, `customers/`).
# MAGIC
# MAGIC El módulo **SDP** (`sdp-workshop/`) usa **`/Volumes/workshop/ingest/raw`** (JSON en las otras carpetas). Tras las tablas 6–7, el mismo notebook **ya escribió** esos JSON alineados con `fact_pedidos_marketplace` (el pipeline SDP sigue leyendo solo archivos; el `JOIN` con `gold` es opcional en análisis).

# COMMAND ----------

try:
    # bronze/silver ya creados en “Preparación Unity Catalog”; idempotente por si se ejecuta esta celda sola
    for _sch in ("bronze", "silver"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_sch}")
        print(f"✅ Esquema {CATALOG}.{_sch}")

    INGEST_RAW = f"/Volumes/{CATALOG}/ingest/raw"
    CSV_GENIE_SUBDIR = "transacciones_core"
    dbutils.fs.mkdirs(f"{INGEST_RAW}/{CSV_GENIE_SUBDIR}")

    EXPORT_DIR = f"{INGEST_RAW}/{CSV_GENIE_SUBDIR}/_export_transacciones_tmp"

    try:
        dbutils.fs.rm(EXPORT_DIR, recurse=True)
    except Exception:
        pass

    # Exportar desde la misma tabla que usa el resto del workshop (cabecera = columnas de fact_transacciones)
    _tx_export = spark.table(f"{CATALOG}.{SCHEMA}.fact_transacciones")
    _tx_export.coalesce(1).write.mode("overwrite").option("header", True).option("compression", "none").csv(EXPORT_DIR)

    parts = [f.path for f in dbutils.fs.ls(EXPORT_DIR) if f.name.startswith("part-") and f.name.endswith(".csv")]
    if not parts:
        raise RuntimeError("No se generó part-*.csv en la exportación")
    parts.sort()
    TARGET_CSV = f"{INGEST_RAW}/{CSV_GENIE_SUBDIR}/transacciones_nuevas.csv"
    dbutils.fs.cp(parts[0], TARGET_CSV)
    dbutils.fs.rm(EXPORT_DIR, recurse=True)
    print(f"✅ CSV exportado (desde gold.fact_transacciones): {TARGET_CSV}")
    print("   Úsalo en el paso Genie 'Pipeline medallión' con spark.read.option('header',true).csv(...)")
except Exception as e:
    print(f"⚠️ No se pudo crear volumen/CSV o esquemas bronze/silver: {e}")
    print("   El facilitador puede crear workshop.ingest.raw, workshop.bronze, workshop.silver y exportar manualmente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de Validación

# COMMAND ----------

print("=" * 65)
print("GENERACIÓN DE DATOS WORKSHOP BANCO GUAYAQUIL — RESUMEN")
print("=" * 65)

tables = [
    "dim_clientes", "dim_sucursales", "fact_transacciones",
    "fact_cartera_creditos", "fact_kpis_diarios",
    "dim_categoria_pedido_digital", "fact_pedidos_marketplace",
]
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.{t}").collect()[0]["cnt"]
    print(f"  {CATALOG}.{SCHEMA}.{t}: {count:,} filas")

_ftg_cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.fact_transacciones_mensual_genie").collect()[0]["cnt"]
print(f"  {CATALOG}.{SCHEMA}.fact_transacciones_mensual_genie: {_ftg_cnt:,} filas (camino Genie; 0 hasta ejecutar paso 2)")
print()
print("Preparación UC (todos los tracks): esquemas ingest / bronze / silver / gold, volumen ingest.raw y carpetas bajo /Volumes/workshop/ingest/raw/.")
print("Complemento marketplace + caminos paralelos:")
print("  dim_categoria_pedido_digital, fact_pedidos_marketplace — semilla alineada con JSON en ingest.")
print("  Tras ejecutar el pipeline SDP: bronze/silver/gold con marketplace_*, fact_marketplace_pedidos_diario, dim_marketplace_* (ver sdp-workshop/transformations/).")
print()
print("Defectos DQ inyectados:")
print("  dim_clientes:         10 null country, 6 fechas futuras, 5 IDs dup, 15 segment minúsculas, 18 scores inválidos")
print("  dim_sucursales:       8 null country, 3 IDs dup, 10 branch_type minúsculas")
print("  fact_transacciones:   150 monto cero, 30 null fecha, 60 clientes huérfanos, 25 montos negativos")
print("  fact_cartera_creditos: 8 DPD negativos, 4 madurez<desembolso, 10 saldo>original, 5 null tasa, 12 bucket inconsistente")
print("  fact_kpis_diarios:    8 NPL>1.0, 5 portfolio=0, 3 centinelas, 6 país DESCONOCIDO")
print()
print(f"Total defectos: ~382")
print()
print("Regiones sintéticas (Ecuador) incluidas:")
for cc, info in COUNTRIES.items():
    print(f"  {cc}: {info['name']} — {info['customers']} clientes, {info['branches']} sucursales, ${info['base_portfolio']/1e6:.0f}M cartera")
print("=" * 65)
print("✅ Generación completa. Workshop listo.")
print()
print("Genie Data Engineering: CSV del core en /Volumes/workshop/ingest/raw/transacciones_core/transacciones_nuevas.csv")
print("Lakeflow SDP (sdp-workshop): JSON en /Volumes/workshop/ingest/raw/ (mismos ORD*/BPC-* que fact_pedidos_marketplace)")
