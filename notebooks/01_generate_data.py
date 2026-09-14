# Databricks notebook source
# MAGIC %md
# MAGIC # Étape 2 — Génération du jeu de données de test
# MAGIC Ce notebook génère un jeu de données synthétique de transactions retail
# MAGIC et le dépose en tant que fichier CSV brut dans un Volume Unity Catalog,
# MAGIC pour simuler l'arrivée de données brutes dans un vrai projet (landing zone).
# MAGIC
# MAGIC Avant d'exécuter ce notebook, crée le catalogue/schéma/volume via l'UI
# MAGIC Catalog Explorer (voir instructions), ou laisse le code ci-dessous les
# MAGIC créer automatiquement si tu as les droits nécessaires.

# COMMAND ----------

# Paramètres du projet — adapte si besoin
CATALOG = "workspace"          # catalogue Unity Catalog à utiliser
SCHEMA = "retail_project"      # schéma dédié à ce projet
VOLUME = "raw_data"            # volume qui simule la landing zone
FILE_NAME = "transactions_raw.csv"

# COMMAND ----------

# Création du schéma et du volume (si non existants)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print(f"Volume prêt : {volume_path}")

# COMMAND ----------

import random
import csv
from datetime import datetime, timedelta

random.seed(42)

# Référentiels simples pour rendre les données réalistes
STORES = [
    (101, "Paris"), (102, "Lyon"), (103, "Marseille"),
    (104, "Toulouse"), (105, "Lille"),
]

CATEGORIES = {
    "Électronique": ["Casque audio", "Chargeur USB-C", "Enceinte Bluetooth", "Souris sans fil"],
    "Épicerie": ["Café moulu", "Pâtes", "Huile d'olive", "Chocolat noir"],
    "Vêtements": ["T-shirt coton", "Jean slim", "Veste légère", "Chaussettes (lot)"],
    "Maison": ["Bougie parfumée", "Plaid polaire", "Vaisselle (set)", "Lampe de bureau"],
    "Sport": ["Tapis de yoga", "Gourde isotherme", "Corde à sauter", "Sac de sport"],
}

PAYMENT_METHODS = ["Carte bancaire", "Espèces", "Sans contact mobile", "Chèque"]

N_ROWS = 8000
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 12, 31)
delta_days = (end_date - start_date).days

rows = []
for i in range(1, N_ROWS + 1):
    tx_date = start_date + timedelta(
        days=random.randint(0, delta_days),
        hours=random.randint(8, 20),
        minutes=random.randint(0, 59),
    )
    store_id, store_region = random.choice(STORES)
    category = random.choice(list(CATEGORIES.keys()))
    product_name = random.choice(CATEGORIES[category])
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(4.99, 149.99), 2)
    customer_id = random.randint(1000, 4999)
    payment_method = random.choice(PAYMENT_METHODS)

    # On introduit volontairement quelques anomalies pour l'étape de nettoyage (Silver)
    if random.random() < 0.02:
        quantity = -1  # quantité invalide
    if random.random() < 0.01:
        unit_price = None  # prix manquant
    if random.random() < 0.01:
        customer_id = None  # client manquant

    rows.append([
        i,
        tx_date.strftime("%Y-%m-%d %H:%M:%S"),
        store_id,
        store_region,
        category,
        product_name,
        quantity,
        unit_price,
        customer_id,
        payment_method,
    ])

header = [
    "transaction_id", "transaction_ts", "store_id", "store_region",
    "product_category", "product_name", "quantity", "unit_price",
    "customer_id", "payment_method",
]

# Les Volumes Unity Catalog sont exposés comme un chemin de fichier standard :
# on écrit donc directement dedans avec un open() Python classique,
# sans passer par /tmp ni par dbutils.fs (bloqué en serverless partagé).
target_file = f"{volume_path}/{FILE_NAME}"
with open(target_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

print(f"{N_ROWS} lignes générées et déposées dans {target_file}")

# COMMAND ----------

# Vérification rapide
df_check = spark.read.option("header", True).csv(f"{volume_path}/{FILE_NAME}")
display(df_check.limit(10))
print(f"Nombre de lignes dans le fichier : {df_check.count()}")
