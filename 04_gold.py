# Databricks notebook source
# MAGIC %md
# MAGIC # Étape 5 — Couche Gold
# MAGIC On part de la table Silver (propre, fiable) pour produire trois tables Gold,
# MAGIC chacune répondant à une question métier différente. C'est le principe du Gold :
# MAGIC des données agrégées, prêtes à consommer directement dans un outil de reporting
# MAGIC (Power BI, dashboard SQL Databricks, etc.) sans transformation supplémentaire.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "retail_project"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_transactions"

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1 — Chiffre d'affaires par jour
# MAGIC Vue temporelle : utile pour suivre la tendance, détecter la saisonnalité.

# COMMAND ----------

df_gold_daily = (
    df_silver
    .withColumn("transaction_date", F.to_date("transaction_ts"))
    .groupBy("transaction_date")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.sum("quantity").alias("total_quantity"),
        F.count("transaction_id").alias("nb_transactions"),
    )
    .orderBy("transaction_date")
)

df_gold_daily.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_sales_daily")
print(f"Table créée : {CATALOG}.{SCHEMA}.gold_sales_daily")
display(df_gold_daily.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2 — Chiffre d'affaires par magasin / région
# MAGIC Vue géographique : utile pour comparer la performance des points de vente.

# COMMAND ----------

df_gold_store = (
    df_silver
    .groupBy("store_id", "store_region")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.sum("quantity").alias("total_quantity"),
        F.count("transaction_id").alias("nb_transactions"),
        F.round(F.avg("total_amount"), 2).alias("panier_moyen"),
    )
    .orderBy(F.desc("revenue"))
)

df_gold_store.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_sales_by_store")
print(f"Table créée : {CATALOG}.{SCHEMA}.gold_sales_by_store")
display(df_gold_store)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 3 — Chiffre d'affaires par catégorie de produit
# MAGIC Vue produit : utile pour identifier les catégories les plus performantes.

# COMMAND ----------

df_gold_category = (
    df_silver
    .groupBy("product_category")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.sum("quantity").alias("total_quantity"),
        F.count("transaction_id").alias("nb_transactions"),
        F.round(F.avg("total_amount"), 2).alias("panier_moyen"),
    )
    .orderBy(F.desc("revenue"))
)

df_gold_category.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_sales_by_category")
print(f"Table créée : {CATALOG}.{SCHEMA}.gold_sales_by_category")
display(df_gold_category)

# COMMAND ----------

# MAGIC %md
# MAGIC Les 3 tables Gold sont prêtes :
# MAGIC - `gold_sales_daily`
# MAGIC - `gold_sales_by_store`
# MAGIC - `gold_sales_by_category`
# MAGIC
# MAGIC Prochaine étape : automatiser l'enchaînement Bronze → Silver → Gold avec un
# MAGIC Workflow Databricks, pour ne plus avoir à exécuter les notebooks à la main.
