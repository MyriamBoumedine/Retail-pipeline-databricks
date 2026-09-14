# Pipeline retail Databricks (Bronze / Silver / Gold)

Pipeline de données retail construit sur Databricks (Unity Catalog), suivant
l'architecture médaillon Bronze → Silver → Gold.

## Contexte

- **Catalogue / schéma** : `workspace.retail_project`
- **Volume (landing zone)** : `workspace.retail_project.raw_data`
- **Compute** : Databricks serverless (Free Edition)

## Structure du repo

```
notebooks/
  01_generate_data.py   # génération de données retail synthétiques (hors job)
  02_bronze.py           # ingestion brute + métadonnées techniques
  03_silver.py           # nettoyage, typage, dédoublonnage
  04_gold.py              # agrégats métier (CA par jour / magasin / catégorie)
  05_governance.py        # lineage, permissions, row-level security
```

## Pipeline de données

1. **01_generate_data.py** — génère ~8000 transactions synthétiques
   (magasins Paris/Lyon/Marseille/Toulouse/Lille) et les écrit dans le Volume
   `raw_data`. Notebook volontairement hors du job orchestré : il simule une
   source externe.
2. **02_bronze.py** — ingère les données brutes (tout en string) dans
   `bronze_transactions`, avec `_ingested_at` et `_source_file`.
3. **03_silver.py** — typage, rejet des lignes invalides (quantité <= 0, prix
   manquant), dédoublonnage sur `transaction_id`, calcul de `total_amount`.
4. **04_gold.py** — produit 3 tables agrégées :
   - `gold_sales_daily` — CA par jour
   - `gold_sales_by_store` — CA par magasin/région + panier moyen
   - `gold_sales_by_category` — CA par catégorie + panier moyen
5. **05_governance.py** — lineage Unity Catalog, permissions (GRANT/REVOKE),
   row-level security via la fonction `region_filter` appliquée sur
   `gold_sales_by_store`.

## Orchestration

Job Databricks `retail_project_pipeline` : 3 tâches enchaînées
(bronze → silver → gold).

## CI/CD (en cours de mise en place)

Ce repo est en cours d'intégration à un pipeline GitLab CI/CD basé sur
Databricks Asset Bundles (`databricks.yml`) pour automatiser la validation et
le déploiement du job.
