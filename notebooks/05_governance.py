# Databricks notebook source
# MAGIC %md
# MAGIC # Étape 7 — Gouvernance Unity Catalog
# MAGIC Deux volets : la gestion des permissions (GRANT/REVOKE), et un exemple de
# MAGIC sécurité au niveau ligne (Row-Level Security) directement dans Databricks.
# MAGIC
# MAGIC Le lineage (traçabilité des données) ne demande aucun code : il se consulte
# MAGIC directement dans Catalog Explorer, onglet "Lineage" de n'importe quelle table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Permissions — syntaxe standard
# MAGIC En mission, on donne généralement un accès complet aux tables Bronze/Silver
# MAGIC uniquement aux ingénieurs data, et un accès en lecture seule aux tables Gold
# MAGIC pour les analystes / outils de reporting (Power BI, par exemple).
# MAGIC
# MAGIC Ces commandes sont données à titre d'exemple — en solo sur un essai gratuit,
# MAGIC tu n'as pas d'autre utilisateur/groupe à qui les appliquer, mais la syntaxe
# MAGIC est strictement celle utilisée en entreprise.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemple : donner un accès lecture seule sur les tables Gold à un groupe "analystes"
# MAGIC -- GRANT SELECT ON SCHEMA workspace.retail_project TO `analystes`;
# MAGIC
# MAGIC -- Exemple : donner un accès complet (lecture/écriture) sur Bronze/Silver aux data engineers
# MAGIC -- GRANT ALL PRIVILEGES ON TABLE workspace.retail_project.bronze_transactions TO `data_engineers`;
# MAGIC -- GRANT ALL PRIVILEGES ON TABLE workspace.retail_project.silver_transactions TO `data_engineers`;
# MAGIC
# MAGIC -- Pour retirer un accès :
# MAGIC -- REVOKE SELECT ON SCHEMA workspace.retail_project FROM `analystes`;
# MAGIC
# MAGIC -- Pour voir les permissions existantes sur un objet :
# MAGIC SHOW GRANTS ON SCHEMA workspace.retail_project;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row-Level Security — exemple sur `gold_sales_by_store`
# MAGIC On crée une fonction SQL qui définit la règle de filtrage, puis on l'applique
# MAGIC à la table comme "row filter". Ici, la règle est volontairement simple :
# MAGIC un utilisateur donné (variable `current_user()`) ne verrait que les données
# MAGIC de la région Paris — le reste du temps (cas par défaut), tout le monde voit tout.
# MAGIC
# MAGIC C'est l'équivalent, côté plateforme de données, du rôle RLS que tu configures
# MAGIC dans Power BI au niveau du modèle sémantique.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION workspace.retail_project.region_filter(store_region STRING)
# MAGIC RETURN
# MAGIC   is_account_group_member('admins')  -- les admins voient tout
# MAGIC   OR current_user() = 'utilisateur.restreint@exemple.com' AND store_region = 'Paris'
# MAGIC   OR current_user() != 'utilisateur.restreint@exemple.com';  -- tout le monde d'autre voit tout (démo)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.retail_project.gold_sales_by_store
# MAGIC SET ROW FILTER workspace.retail_project.region_filter ON (store_region);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vérification : la requête s'exécute normalement, le filtre est transparent
# MAGIC -- pour l'utilisateur (il ne voit tout simplement pas les lignes exclues)
# MAGIC SELECT * FROM workspace.retail_project.gold_sales_by_store;

# COMMAND ----------

# MAGIC %md
# MAGIC Pour retirer le filtre (utile si ça bloque la suite du projet) :
# MAGIC ```sql
# MAGIC ALTER TABLE workspace.retail_project.gold_sales_by_store DROP ROW FILTER;
# MAGIC ```
