# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "351803b6-433f-46dd-8c25-6b5279687ada",
# META       "default_lakehouse_name": "Lakehouse_silver",
# META       "default_lakehouse_workspace_id": "ddd41291-ce79-4d22-88f4-8929794fb662",
# META       "known_lakehouses": [
# META         {
# META           "id": "351803b6-433f-46dd-8c25-6b5279687ada"
# META         },
# META         {
# META           "id": "d1c18e0f-b37d-4ce0-9bcd-d1cab1281661"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Lakehouse_bronze.bronze.consumption_raw")
df.show(10)
print(f"📊 Données Bronze : {df.count()} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS Lakehouse_silver.silver")
print('Le schéma est prêt')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Confirmer le lakehouse actif le Lakehouse_bronze
df_schemas = spark.sql("SHOW SCHEMAS").toPandas()
print(f"✅ Schémas disponibles : {list(df_schemas.iloc[:,0])}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dedup = spark.sql("SELECT DISTINCT * FROM Lakehouse_bronze.bronze.consumption_raw")
df_dedup.write.mode("overwrite").format("delta").saveAsTable("Lakehouse_silver.silver.consumption_dedup")
print(f"✅ Table silver.consumption_dedup créée : {df_dedup.count()} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_clean = spark.table("Lakehouse_silver.silver.consumption_dedup").filter(
    (F.col("consumption_mw").isNotNull()) &  # Pas de NULL
    (F.col("consumption_mw") >= 0) &         # Pas de codes erreur négatifs
    (F.col("consumption_mw") < 10)           # Pas d'outliers
)

df_clean.write.mode("overwrite").format("delta").saveAsTable("Lakehouse_silver.silver.consumption_clean")
print(f"✅ Table silver.consumption_clean créée : {df_clean.count()} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compter les lignes
original_count = spark.sql("SELECT COUNT(*) FROM Lakehouse_bronze.bronze.consumption_raw").collect()[0][0]
clean_count = spark.table("Lakehouse_silver.silver.consumption_clean").count()
removed_count = original_count - clean_count

print("="*50)
print("📊 VÉRIFICATION DU NETTOYAGE")
print("="*50)
print(f"Lignes originales   : {original_count:,}")
print(f"Lignes nettoyées    : {clean_count:,}")
print(f"Lignes supprimées   : {removed_count:,} ({removed_count/original_count*100:.1f}%)")
print("="*50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("Lakehouse_silver.silver.consumption_clean")

# Normalisation formats de dates
df = df.withColumn(
    "timestamp_clean",
    F.coalesce(
        F.to_timestamp(F.col("timestamp")),                            # ISO 8601
        F.to_timestamp(F.col("timestamp"), "dd/MM/yyyy HH:mm:ss"),     # Français
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")      # Américain
    )
)

# Conserver les autres colonnes
df = df.select("site_id", "consumption_mw", "voltage_v", "frequency_hz", "status", "timestamp_clean")

df.write.mode("overwrite").format("delta").saveAsTable("Lakehouse_silver.silver.consumption_dates_fixed")
print(f"✅ Table silver.consumption_dates_fixed créée : {df.count()} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("Lakehouse_silver.silver.consumption_dates_fixed").filter(
    F.col("timestamp_clean").isNotNull()
)

df_hourly = df.groupBy(
    F.date_trunc("hour", "timestamp_clean").alias("hour"),
    "site_id"
).agg(
    F.avg("consumption_mw").alias("avg_consumption_mw"),
    F.max("consumption_mw").alias("max_consumption_mw"),
    F.min("consumption_mw").alias("min_consumption_mw"),
    F.count("*").alias("measurements")
)

df_hourly.write.mode("overwrite").format("delta").saveAsTable("Lakehouse_silver.silver.consumption_hourly")
print(f"✅ Table silver.consumption_hourly créée : {df_hourly.count()} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_conso = spark.table("Lakehouse_silver.silver.consumption_hourly")
df_prix = spark.sql("SELECT * FROM Lakehouse_bronze.bronze.market_prices")

# Convertir timestamp en hour pour le prix
df_prix = df_prix.withColumn("hour", F.date_trunc("hour", F.col("timestamp").cast("timestamp")))

# Jointure
df_with_prices = df_conso.join(
    df_prix.select("hour", "price_eur_mwh", "market"),
    on="hour",
    how="left"
)

df_with_prices.write.mode("overwrite").format("delta").saveAsTable("Lakehouse_silver.silver.consumption_with_prices")
print(f"✅ Table silver.consumption_with_prices créée : {df_with_prices.count()} lignes")
df_with_prices.show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# La table silver.consumption_with_prices existe déjà (créée en cellule 7)
# On vérifie juste qu'elle est bien là
df = spark.table("Lakehouse_silver.silver.consumption_with_prices")
print(f"✅ Table silver.consumption_with_prices sauvegardée : {df.count()} lignes")
print("\n📊 Aperçu des données nettoyées :")
df.show(10)
print("\n➡️ Prochaine étape : Notebook 3 (PySpark - Calculs avancés)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

# ─── 1. Charger la base ───────────────────────────────────────────────
df = spark.table("Lakehouse_silver.silver.consumption_with_prices")

# ─── 2. Baseline 7 jours glissants ───────────────────────────────────
w7 = Window.partitionBy("site_id") \
           .orderBy(F.col("hour").cast("long")) \
           .rangeBetween(-7 * 24 * 3600, -1)

df = df.withColumn("baseline_7d_mw", F.avg("avg_consumption_mw").over(w7))

# ─── 3. Ratio vs baseline ─────────────────────────────────────────────
df = df.withColumn(
    "ratio_vs_baseline",
    F.when(F.col("baseline_7d_mw") > 0,
           F.col("avg_consumption_mw") / F.col("baseline_7d_mw")
    ).otherwise(None)
)

# ─── 4. Z-score et détection anomalies ───────────────────────────────
w_site = Window.partitionBy("site_id")

df = df.withColumn("mean_site", F.avg("avg_consumption_mw").over(w_site)) \
       .withColumn("std_site",  F.stddev("avg_consumption_mw").over(w_site)) \
       .withColumn("z_score",
           F.when(F.col("std_site") > 0,
               (F.col("avg_consumption_mw") - F.col("mean_site")) / F.col("std_site")
           ).otherwise(0)
       ) \
       .withColumn("anomaly",
           F.when(F.abs(F.col("z_score")) > 3, "⚠️ ANOMALIE").otherwise("✅ Normal")
       ) \
       .drop("mean_site", "std_site")

# ─── 5. Features temporelles ─────────────────────────────────────────
df = df.withColumn("hour_of_day", F.hour("hour")) \
       .withColumn("day_of_week", F.dayofweek("hour")) \
       .withColumn("is_weekend",  F.dayofweek("hour").isin(1, 7))

# ─── 6. Jointure référentiel sites ───────────────────────────────────
df_sites = spark.sql("SELECT * FROM Lakehouse_bronze.bronze.sites_reference")
df = df.join(df_sites, on="site_id", how="left")

# ─── 7. Jointure météo (globale, pas de site_id) ─────────────────────
df_weather = spark.sql("SELECT * FROM Lakehouse_bronze.bronze.weather_data") \
                  .withColumn("hour_w", F.date_trunc("hour", F.col("timestamp").cast("timestamp"))) \
                  .select("hour_w", "temperature_c", "wind_speed_ms")

df = df.join(df_weather, df["hour"] == df_weather["hour_w"], how="left") \
       .drop("hour_w")

# ─── 8. Flag maintenance (table absente → False par défaut) ───────────
df = df.withColumn("in_maintenance", F.lit(False))

# ─── 9. Features ML : lag J-1 et J-7 ────────────────────────────────
w_lag = Window.partitionBy("site_id").orderBy("hour")

df = df.withColumn("lag_24h",  F.lag("avg_consumption_mw", 24).over(w_lag)) \
       .withColumn("lag_168h", F.lag("avg_consumption_mw", 168).over(w_lag))

# ─── 10. Colonne prediction (placeholder) ────────────────────────────
df = df.withColumn("prediction", F.col("baseline_7d_mw"))

# ─── 11. Sauvegarde ───────────────────────────────────────────────────
df.write.mode("overwrite").format("delta") \
  .saveAsTable("Lakehouse_silver.silver.consumption_enriched")

count = df.count()
print(f"✅ Table silver.consumption_enriched créée : {count} lignes")
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

df = spark.table("Lakehouse_silver.silver.consumption_enriched")

# Fenêtre par site ordonnée par heure
w_lag = Window.partitionBy("site_id").orderBy("hour")

# Prédiction = moyenne pondérée baseline 7j + lag 24h + lag 168h
df_pred = df.withColumn("lag_24h",  F.lag("avg_consumption_mw", 24).over(w_lag)) \
            .withColumn("lag_168h", F.lag("avg_consumption_mw", 168).over(w_lag)) \
            .withColumn(
                "prediction",
                F.when(
                    F.col("lag_24h").isNotNull() & F.col("lag_168h").isNotNull(),
                    F.round(
                        F.col("baseline_7d_mw") * 0.5 +
                        F.col("lag_24h")         * 0.3 +
                        F.col("lag_168h")         * 0.2,
                        4
                    )
                ).otherwise(F.col("baseline_7d_mw"))  # fallback si pas assez d'historique
            ) \
            .withColumn(
                "prediction_error_mw",
                F.round(F.abs(F.col("avg_consumption_mw") - F.col("prediction")), 4)
            ) \
            .withColumn(
                "prediction_error_pct",
                F.when(
                    F.col("avg_consumption_mw") > 0,
                    F.round(F.col("prediction_error_mw") / F.col("avg_consumption_mw") * 100, 2)
                ).otherwise(None)
            )

# Sélection des colonnes utiles pour la table predictions
df_pred_final = df_pred.select(
    "hour", "site_id",
    "avg_consumption_mw",
    "prediction",
    "prediction_error_mw",
    "prediction_error_pct",
    "lag_24h",
    "lag_168h"
)

df_pred_final.write.mode("overwrite").format("delta") \
    .saveAsTable("Lakehouse_silver.silver.consumption_predictions")

print(f"✅ silver.consumption_predictions créée : {df_pred_final.count():,} lignes")
df_pred_final.show(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.sort(sf.asc("hour")).show(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pred_final.orderBy(F.col("hour").desc()).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
