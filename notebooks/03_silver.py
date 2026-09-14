# Databricks notebook source
# MAGIC %md
# MAGIC # Étape 4 — Couche Silver
# MAGIC On part de la table Bronze (tout en string, brut) pour produire une table
# MAGIC Silver propre et fiable : typage correct, gestion des anomalies, dédoublonnage,
# MAGIC et ajout d'une colonne dérivée utile (montant total).
# MAGIC
# MAGIC Règles de qualité appliquées :
# MAGIC - `quantity` doit être strictement positive, sinon la ligne est rejetée
# MAGIC - `unit_price` doit être renseigné, sinon la ligne est rejetée (on ne peut pas
# MAGIC   calculer de chiffre d'affaires sans prix)
# MAGIC - `customer_id` manquant est toléré (vente anonyme) : on le remplace par
# MAGIC   une valeur "UNKNOWN" plutôt que de rejeter la ligne
# MAGIC - les doublons sur `transaction_id` sont supprimés (défensif, au cas où)

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "retail_project"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_transactions"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_transactions"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

df_bronze = spark.table(BRONZE_TABLE)
n_input = df_bronze.count()

# COMMAND ----------

# Typage explicite (la table Bronze est entièrement en string)
df_typed = (
    df_bronze
    .withColumn("transaction_id", F.col("transaction_id").cast(IntegerType()))
    .withColumn("transaction_ts", F.col("transaction_ts").cast(TimestampType()))
    .withColumn("store_id", F.col("store_id").cast(IntegerType()))
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
)

# COMMAND ----------

# Comptage des anomalies avant nettoyage (utile pour un rapport de qualité de données)
n_invalid_quantity = df_typed.filter((F.col("quantity").isNull()) | (F.col("quantity") <= 0)).count()
n_missing_price = df_typed.filter(F.col("unit_price").isNull()).count()
n_missing_customer = df_typed.filter(F.col("customer_id").isNull()).count()

print(f"Lignes en entrée (Bronze)      : {n_input}")
print(f"Quantité invalide (rejetées)   : {n_invalid_quantity}")
print(f"Prix manquant (rejetées)       : {n_missing_price}")
print(f"Client inconnu (conservées)    : {n_missing_customer}")

# COMMAND ----------

# Application des règles de qualité
df_clean = (
    df_typed
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price").isNotNull())
    .fillna({"customer_id": -1})  # -1 = client anonyme / inconnu
    .dropDuplicates(["transaction_id"])
    .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .drop("_ingested_at", "_source_file")  # métadonnées Bronze, pas utiles en Silver
    .withColumn("_processed_at", F.current_timestamp())
)

n_output = df_clean.count()
print(f"Lignes en sortie (Silver)      : {n_output}")

# COMMAND ----------

df_clean.write.mode("overwrite").saveAsTable(SILVER_TABLE)

print(f"Table Silver créée : {SILVER_TABLE}")
display(spark.table(SILVER_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vérification rapide
# MAGIC On s'assure qu'il ne reste plus d'anomalie dans la table Silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_lignes,
# MAGIC   SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) AS quantites_invalides_restantes,
# MAGIC   SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS prix_manquants_restants
# MAGIC FROM workspace.retail_project.silver_transactions
