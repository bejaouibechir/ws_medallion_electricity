# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d1c18e0f-b37d-4ce0-9bcd-d1cab1281661",
# META       "default_lakehouse_name": "Lakehouse_bronze",
# META       "default_lakehouse_workspace_id": "ddd41291-ce79-4d22-88f4-8929794fb662",
# META       "known_lakehouses": [
# META         {
# META           "id": "d1c18e0f-b37d-4ce0-9bcd-d1cab1281661"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Vérifier que Spark est disponible
print(f"✅ Spark version: {spark.version}")

# Créer le schéma bronze s'il n'existe pas encore
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
print("✅ Schéma bronze prêt")

# Confirmer le lakehouse actif
df_schemas = spark.sql("SHOW SCHEMAS").toPandas()
print(f"✅ Schémas disponibles : {list(df_schemas.iloc[:,0])}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Charger le CSV avec PySpark
df_consumption = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/data_bronze_notebooks/consumption_raw.csv")

# Compter les lignes avant écriture
nb_lignes = df_consumption.count()

# Créer la table dans le schéma bronze
df_consumption.write.mode("overwrite").format("delta").saveAsTable("bronze.consumption_raw")

print(f"✅ Table bronze.consumption_raw créée : {nb_lignes} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM bronze.consumption_raw LIMIT 20

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Compter les NULL, codes erreur, doublons
# MAGIC SELECT
# MAGIC     COUNT(*) as total_rows,
# MAGIC     COUNT(DISTINCT timestamp, site_id) as unique_rows,
# MAGIC     COUNT(*) - COUNT(DISTINCT timestamp, site_id) as duplicates,
# MAGIC     SUM(CASE WHEN consumption_mw IS NULL THEN 1 ELSE 0 END) as null_values,
# MAGIC     SUM(CASE WHEN consumption_mw < 0 THEN 1 ELSE 0 END) as error_codes,
# MAGIC     SUM(CASE WHEN consumption_mw > 10 THEN 1 ELSE 0 END) as outliers
# MAGIC FROM bronze.consumption_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Market prices
spark.read.csv("Files/data_bronze_notebooks/market_prices.csv", header=True, inferSchema=True) \
    .write.mode("overwrite").format("delta").saveAsTable("bronze.market_prices")

# Weather
spark.read.csv("Files/data_bronze_notebooks/weather_data.csv", header=True, inferSchema=True) \
    .write.mode("overwrite").format("delta").saveAsTable("bronze.weather_data")

# Sites reference
spark.read.csv("Files/data_bronze_notebooks/sites_reference.csv", header=True, inferSchema=True) \
    .write.mode("overwrite").format("delta").saveAsTable("bronze.sites_reference")

# Maintenance events
spark.read.csv("Files/data_bronze_notebooks/maintenance_events.csv", header=True, inferSchema=True) \
    .write.mode("overwrite").format("delta").saveAsTable("bronze.maintenance_events")

print("✅ Toutes les tables Bronze créées")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'consumption' as table_name, COUNT(*) as rows FROM bronze.consumption_raw
# MAGIC UNION ALL
# MAGIC SELECT 'prices', COUNT(*) FROM bronze.market_prices
# MAGIC UNION ALL
# MAGIC SELECT 'weather', COUNT(*) FROM bronze.weather_data
# MAGIC UNION ALL
# MAGIC SELECT 'sites', COUNT(*) FROM bronze.sites_reference
# MAGIC UNION ALL
# MAGIC SELECT 'maintenance', COUNT(*) FROM bronze.maintenance_events

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
rapport_bronze = f"""
╔════════════════════════════════════════════════════════════════════╗
║                  RAPPORT QUALITÉ COUCHE BRONZE                     ║
║                   Pipeline Medallion - Électricité                 ║
╚════════════════════════════════════════════════════════════════════╝

📊 RÉSUMÉ DE L'INGESTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ 5 fichiers ingérés avec succès
✅ 5 tables Delta créées dans le schéma 'bronze'
✅ 21,758 lignes totales stockées (consommation + métadonnées)

📋 TABLES CRÉÉES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• bronze.consumption_raw      : 18,144 lignes (données IoT)
• bronze.market_prices        :  2,880 lignes (prix spot EPEX)
• bronze.weather_data         :    720 lignes (conditions météo)
• bronze.sites_reference      :      6 lignes (données maîtres sites)
• bronze.maintenance_events   :      8 lignes (événements maintenance)

⚠️ ANOMALIES DÉTECTÉES DANS bronze.consumption_raw
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔴 DOUBLONS : 864 lignes (4.8%)
   └─ Cause probable : retransmissions réseau, recalibrage capteur
   └─ Action Silver : Dédupliquer en préservant la dernière mesure

🔴 VALEURS NULL : 554 lignes (3.0%)
   └─ Cause probable : Défaillance capteur, panne réseau
   └─ Action Silver : Filtrer ou imputer (interpolation linéaire)

🔴 CODES ERREUR : 319 lignes (1.8%)
   └─ Codes détectés : -888 (défaut capteur), -999 (panne réseau), -777 (calibrage)
   └─ Action Silver : Mapper à catégories métier, puis filtrer

🔴 OUTLIERS : 27 lignes (0.15%)
   └─ Consommation > 10 MW (dépassant capacité max théorique 5 MW)
   └─ Action Silver : Investigation détaillée, rejet ou correction d'ordre de grandeur

🟡 FORMATS TIMESTAMPS HÉTÉROGÈNES : 3+ formats détectés
   └─ ISO 8601 (2025-12-01T00:15:00Z)
   └─ Français (01/12/2025 01:15)
   └─ Mixte (2025-12-01/12/01 00:15)
   └─ Action Silver : Normaliser à ISO 8601, valider cohérence

📈 TAUX DE QUALITÉ GLOBAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Lignes sans anomalie identifiée : 16,280 / 18,144 = 89.7% ✅
Lignes avec au moins une anomalie : 1,864 / 18,144 = 10.3% ⚠️

ÉVALUATION : Taux acceptable pour données IoT en production brute.
            Nettoyage Silver ciblé sur 10% des données.

🎯 PRIORITÉS SILVER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1️⃣  Normaliser timestamps (ISO 8601, fuseau horaire)
2️⃣  Dédupliquer (864 doublons)
3️⃣  Traiter codes erreur (-888, -999, -777) → filtrer ou imputer
4️⃣  Gérer NULL (interpolation ou exclusion)
5️⃣  Valider outliers (27 mesures > 10 MW)
6️⃣  Enrichir avec données sites_reference, weather, maintenance

SOURCE DE VÉRITÉ : Immutable
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Tous les fichiers bruts conservés intégralement en Bronze.
✅ Aucune transformation appliquée. Audit trail complet maintenu.
✅ Tables prêtes pour exploitation par Silver.

Rapport généré : {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

print(rapport_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
