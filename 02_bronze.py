# Databricks notebook source
# MAGIC %md
# MAGIC # Étape 3 — Couche Bronze
# MAGIC On lit le CSV brut déposé dans le Volume (landing zone) et on l'écrit tel quel
# MAGIC dans une table Delta, sans transformation métier. C'est le principe du Bronze :
# MAGIC on garde une copie fidèle de la source, avec juste quelques métadonnées
# MAGIC techniques (date d'ingestion, fichier source), pour pouvoir toujours revenir
# MAGIC à la donnée brute si besoin.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "retail_project"
VOLUME = "raw_data"
FILE_NAME = "transactions_raw.csv"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_transactions"

volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
source_file = f"{volume_path}/{FILE_NAME}"

# COMMAND ----------

from pyspark.sql import functions as F

# Lecture brute : tout est lu en string, on ne fait aucun cast ni nettoyage ici.
# C'est volontaire — le nettoyage et le typage arrivent seulement en couche Silver.
df_raw = (
    spark.read
    .option("header", True)
    .csv(source_file)
)

df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))  # UC : input_file_name() n'est pas supporté
)

# COMMAND ----------

# Écriture en table Delta managée par Unity Catalog
df_bronze.write.mode("overwrite").saveAsTable(BRONZE_TABLE)

print(f"Table Bronze créée : {BRONZE_TABLE}")
display(spark.table(BRONZE_TABLE).limit(10))
print(f"Nombre de lignes : {spark.table(BRONZE_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus — Time travel Delta
# MAGIC Une des forces de Delta Lake : chaque écriture est versionnée. On peut consulter
# MAGIC l'historique des versions d'une table, et même interroger une version passée.
# MAGIC C'est un point à bien maîtriser pour un projet client (audit, rollback, debug).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.retail_project.bronze_transactions
