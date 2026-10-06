# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "df9451c7-210f-467f-805d-cd0b5e3e84f8",
# META       "default_lakehouse_name": "Lakehouse_gold",
# META       "default_lakehouse_workspace_id": "ddd41291-ce79-4d22-88f4-8929794fb662",
# META       "known_lakehouses": [
# META         {
# META           "id": "df9451c7-210f-467f-805d-cd0b5e3e84f8"
# META         },
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

# Lit la table Silver et ne garde que la colonne site_id
# distinct() supprime les doublons : on obtient la liste des sites
df_sites = spark.table("Lakehouse_silver.silver.consumption_enriched").select("site_id").distinct()

print(f"📋 Sites présents dans Silver : {df_sites.count()}")
df_sites.orderBy("site_id").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# count() = nombre de lignes de chaque table source
t1 = spark.table("Lakehouse_silver.silver.consumption_predictions").count()   # prédictions (Silver)
t2 = spark.table("Lakehouse_bronze.bronze.sites_reference").count()           # référentiel des sites (Bronze)

print(f"✅ consumption_predictions : {t1:,} lignes")
print(f"✅ sites_reference         : {t2} lignes")
print("➡️  Prêt pour l'atelier Gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Crée le schéma "gold" dans le lakehouse par défaut s'il n'existe pas encore
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
print("✅ Schéma gold prêt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fonctions Spark (sum, avg, when, col, round...) importées sous l'alias F
from pyspark.sql import functions as F
print("✅ Fonctions PySpark importées")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Charge la table Silver enrichie en DataFrame (rien n'est calculé avant le count)
df_enriched = spark.table("Lakehouse_silver.silver.consumption_enriched")
print(f"📊 consumption_enriched : {df_enriched.count():,} lignes | {len(df_enriched.columns)} colonnes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Charge les prédictions du modèle (une ligne par heure et par site)
df_predictions = spark.table("Lakehouse_silver.silver.consumption_predictions")
print(f"📊 consumption_predictions : {df_predictions.count():,} lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Jointure sur (hour, site_id) : on ajoute prédiction et erreur à chaque mesure réelle
# left join = on garde toutes les mesures, même sans prédiction
df = df_enriched.join(
    df_predictions.select("hour", "site_id",
                          F.col("prediction").alias("predicted_consumption_mw"),  # renommage pour la clarté
                          "prediction_error_mw"),
    on=["hour", "site_id"],
    how="left"
)
print(f"📊 Dataset Gold source : {df.count():,} lignes (attendu : 4 320)")

# Aperçu des colonnes utiles pour la suite (100 premières lignes)
df.select("hour", "site_id", "avg_consumption_mw",
          "predicted_consumption_mw", "prediction_error_mw", "anomaly", "price_eur_mwh").show(100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.filter(df.anomaly != "✅ Normal" ).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Étape 1 : ajouter site_type, capacité, région, flexibilité depuis le référentiel ---
df_sites_ref = spark.table("Lakehouse_bronze.bronze.sites_reference")

# Dropper explicitement les colonnes dupliquées de df avant la jointure
cols_to_drop = [c for c in ["site_type", "capacity_mw", "region", "flexible"] if c in df.columns]
df = df.drop(*cols_to_drop)

# Jointure propre
df = df.join(
    df_sites_ref.select("site_id", "site_type", "capacity_mw", "region", "flexible"),
    on="site_id", how="left"
)

# Vérification
print("Colonnes après jointure:", df.columns)

# --- Étape 2 : agréger l'horaire en journalier (une ligne par jour et par site) ---
df_daily = df.groupBy(
    F.to_date("hour").alias("date"),                 # heure -> jour
    "site_id", "site_type", "capacity_mw"
).agg(
    F.sum("avg_consumption_mw").alias("total_consumption_mwh"),   # énergie du jour (somme des 24 h)
    F.avg("avg_consumption_mw").alias("avg_consumption_mw"),      # puissance moyenne
    F.max("max_consumption_mw").alias("peak_consumption_mw"),     # pic de charge
    F.avg("baseline_7d_mw").alias("avg_baseline_mw"),             # référence 7 jours
    F.avg("predicted_consumption_mw").alias("avg_predicted_mw"),  # prédiction moyenne
    F.avg("prediction_error_mw").alias("avg_prediction_error_mw"),
    F.avg("price_eur_mwh").alias("avg_price_eur_mwh"),
    F.max("price_eur_mwh").alias("peak_price_eur_mwh"),
    F.avg("temperature_c").alias("avg_temperature_c"),
    F.sum(F.when(F.col("anomaly") == "⚠️ ANOMALIE", 1).otherwise(0)).alias("nb_anomalies"),  # heures anormales
    F.count("*").alias("nb_measurements")                         # nombre d'heures du jour
).orderBy(F.desc("date"), "site_id")

print(f"✅ df_daily prêt : {df_daily.count()} lignes (attendu : 6 sites × 30 jours = 180)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Écrit df_daily en table Delta ; overwrite = remplace la table à chaque exécution
df_daily.write.mode("overwrite").format("delta").saveAsTable("gold.daily_consumption_by_site")
print(f"✅ gold.daily_consumption_by_site : {df_daily.count()} lignes")

# Aperçu : 6 lignes du jour le plus récent
df_daily.show(6)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# KPIs mensuels : on regroupe les lignes journalières par site
df_kpis = df_daily.groupBy("site_id", "site_type", "capacity_mw").agg(
    F.sum("total_consumption_mwh").alias("total_consumption_month_mwh"),   # énergie du mois
    F.avg("avg_consumption_mw").alias("avg_consumption_mw"),
    # Facteur de charge = puissance moyenne / capacité × 100
    F.round(F.avg("avg_consumption_mw") / F.col("capacity_mw") * 100, 2).alias("load_factor_pct"),
    # Taux d'anomalies = heures anormales / heures mesurées × 100
    F.round(F.sum("nb_anomalies") * 100.0 / F.sum("nb_measurements"), 2).alias("anomaly_rate_pct"),
    F.round(F.avg("avg_prediction_error_mw"), 3).alias("avg_prediction_error_mw"),
    # Erreur de prédiction relative à la consommation moyenne (%)
    F.round(F.avg("avg_prediction_error_mw") / F.avg("avg_consumption_mw") * 100, 1).alias("prediction_error_pct"),
    F.round(F.avg("avg_price_eur_mwh"), 2).alias("avg_electricity_price_eur_mwh"),
    F.countDistinct("date").alias("nb_days")                               # jours couverts
).orderBy(F.desc("total_consumption_month_mwh"))

# Cette cellule ne calcule que le DataFrame : on affiche donc un résumé lisible
print(f"✅ df_kpis calculé : {df_kpis.count()} lignes (1 par site)")
df_kpis.select("site_id", "total_consumption_month_mwh", "load_factor_pct",
               "anomaly_rate_pct", "prediction_error_pct").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ajoute la région et le flag "flexible" aux KPIs (jointure sur site_id)
df_sites_ref = spark.table("Lakehouse_bronze.bronze.sites_reference")
df_kpis = df_kpis.join(
    df_sites_ref.select("site_id", "region", "flexible"),
    on="site_id", how="left"
)

# Contrôle : colonnes ajoutées et sites éligibles à l'effacement
print("✅ Colonnes ajoutées : region, flexible")
print("🔌 Sites flexibles :", [r["site_id"] for r in df_kpis.filter("flexible = true").select("site_id").orderBy("site_id").collect()])
df_kpis.select("site_id", "region", "flexible").orderBy("site_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Écrit les KPIs en table Delta (overwrite = ré-exécutable)
df_kpis.write.mode("overwrite").format("delta").saveAsTable("gold.site_kpis")
print(f"✅ gold.site_kpis : {df_kpis.count()} lignes")

# Affiche la table complète (6 lignes)
df_kpis.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agrégation par jour et par type de site (Industrie / Commercial / Résidentiel)
df_by_type = df.groupBy(
    F.to_date("hour").alias("date"),
    "site_type"
).agg(
    F.sum("avg_consumption_mw").alias("total_consumption_mwh"),
    F.avg("avg_consumption_mw").alias("avg_consumption_mw"),
    F.sum("capacity_mw").alias("total_capacity_mw"),    # capacité cumulée (ligne horaire × sites)
    # Load factor du segment = consommation / capacité × 100
    F.round(F.sum("avg_consumption_mw") / F.sum("capacity_mw") * 100, 2).alias("load_factor_pct"),
    F.avg("price_eur_mwh").alias("avg_price_eur_mwh"),
    F.countDistinct("site_id").alias("nb_sites")        # sites dans le segment
).orderBy(F.desc("date"), "site_type")

# Sauvegarde Delta
df_by_type.write.mode("overwrite").format("delta").saveAsTable("gold.consumption_by_site_type")
print(f"✅ gold.consumption_by_site_type : {df_by_type.count()} lignes (attendu : 3 types × 30 jours = 90)")

# Aperçu des 3 segments pour le dernier jour
df_by_type.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ne garde que les sites flexibles ET une consommation > 0,5 MW (évite le bruit)
df_flex = df.filter(
    (F.col("flexible") == True) &
    (F.col("avg_consumption_mw") > 0.5)
)
print(f"📊 Lignes éligibles effacement : {df_flex.count():,}")

# Contrôle : quels sites sont retenus et combien de lignes chacun
df_flex.groupBy("site_id").count().orderBy("site_id").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calcul des indicateurs d'effacement sur chaque ligne éligible
df_curtailment = df_flex.withColumn(
    "ratio_vs_baseline",                                      # >1 = consomme plus que d'habitude
    F.round(F.col("avg_consumption_mw") / F.col("baseline_7d_mw"), 2)
).withColumn(
    "curtailment_potential_mw",                               # puissance effaçable
    F.round(F.col("avg_consumption_mw") * 0.3, 3)         # 30% effacement
).withColumn(
    "potential_gain_eur",                                     # gain seulement si prix spot > seuil contractuel
    F.when(
        F.col("price_eur_mwh") > F.col("curtailment_price_eur_mwh"),
        F.round((F.col("avg_consumption_mw") * 0.3) *
                (F.col("price_eur_mwh") - F.col("curtailment_price_eur_mwh")), 2)
    ).otherwise(0)
).withColumn(
    "action_signal",                                          # recommandation selon le prix spot
    F.when(F.col("price_eur_mwh") > 300, "🔴 EFFACEMENT MAX")
     .when(F.col("price_eur_mwh") > 200, "🟠 EFFACEMENT PARTIEL")
     .when(F.col("price_eur_mwh") < 50,  "🟢 CONSOMMER")
     .otherwise("⚪ Surveiller")
)

# Cette cellule ne fait que calculer : on affiche donc un résumé
print(f"✅ Colonnes ajoutées : ratio_vs_baseline, curtailment_potential_mw, potential_gain_eur, action_signal")
print(f"📊 Heures évaluées : {df_curtailment.count():,}")
print("📊 Répartition des signaux d'action :")
df_curtailment.groupBy("action_signal").count().orderBy(F.desc("count")).show(truncate=False)
gain = df_curtailment.agg(F.round(F.sum("potential_gain_eur"), 0)).collect()[0][0]
print(f"💶 Gain potentiel total : {gain:,.0f} €")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sélectionne et ordonne les colonnes utiles, triées par gain décroissant
df_curtailment = df_curtailment.select(
    "hour", "site_id", "site_type", "curtailment_price_eur_mwh",
    "avg_consumption_mw", "baseline_7d_mw", "ratio_vs_baseline",
    "price_eur_mwh", "curtailment_potential_mw", "potential_gain_eur", "action_signal"
).orderBy(F.desc("potential_gain_eur"))

# Sauvegarde Delta
df_curtailment.write.mode("overwrite").format("delta").saveAsTable("gold.curtailment_opportunities")
print(f"✅ gold.curtailment_opportunities : {df_curtailment.count():,} lignes")

# Résumé : heures rentables (gain > 0) et meilleure opportunité
nb_gain = df_curtailment.filter("potential_gain_eur > 0").count()
best = df_curtailment.first()
print(f"💶 Heures avec gain > 0 : {nb_gain:,} sur {df_curtailment.count():,}")
print(f"🏆 Meilleure opportunité : {best['site_id']} le {best['hour']} → {best['potential_gain_eur']} €")

# Aperçu des 50 meilleures heures
df_curtailment.filter("potential_gain_eur > 0").show(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Synthèse mensuelle : une ligne par site, par année et par mois
df_monthly = df_daily.groupBy(
    "site_id", "site_type",
    F.year("date").alias("year"),
    F.month("date").alias("month")
).agg(
    F.sum("total_consumption_mwh").alias("total_mwh"),                    # énergie du mois
    F.avg(F.col("avg_consumption_mw") / F.col("capacity_mw") * 100).alias("avg_load_factor_pct"),
    F.avg("avg_price_eur_mwh").alias("avg_price_eur_mwh"),
    F.sum("nb_anomalies").alias("total_anomalies")                        # heures anormales du mois
).orderBy("site_id")

# Sauvegarde Delta
df_monthly.write.mode("overwrite").format("delta").saveAsTable("gold.monthly_consumption_pivot")
print(f"✅ gold.monthly_consumption_pivot : {df_monthly.count()} lignes")
df_monthly.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Garde les heures qui remplissent AU MOINS UN des 3 critères (OU logique "|")
df_alerts = df.filter(
    (F.col("anomaly") == "⚠️ ANOMALIE") |          # 1. anomalie détectée en Silver
    (F.col("price_eur_mwh") > 300)        |          # 2. prix spot extrême
    (F.col("prediction_error_mw") > 1.0)             # 3. modèle de prédiction en défaut
)
print(f"📊 Événements d'alerte : {df_alerts.count():,}")

# Détail par critère (une heure peut compter dans plusieurs)
print(f"   • anomalies            : {df_alerts.filter(F.col('anomaly') == '⚠️ ANOMALIE').count():,}")
print(f"   • prix > 300 €/MWh     : {df_alerts.filter('price_eur_mwh > 300').count():,}")
print(f"   • erreur prédiction > 1 MW : {df_alerts.filter('prediction_error_mw > 1.0').count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# alert_type : le 1er critère vrai l'emporte (ordre des when)
# priority   : CRITIQUE (anomalie + prix) > HAUTE (l'un des deux) > MOYENNE (erreur de prédiction seule)
df_alerts = df_alerts.withColumn(
    "alert_type",
    F.when(F.col("anomaly") == "⚠️ ANOMALIE", "Consommation anormale")
     .when(F.col("price_eur_mwh") > 300,        "Prix spot très élevé")
     .when(F.col("prediction_error_mw") > 1.0,  "Erreur prédiction élevée")
     .otherwise("Autre")
).withColumn(
    "priority",
    F.when((F.col("anomaly") == "⚠️ ANOMALIE") & (F.col("price_eur_mwh") > 300), "CRITIQUE")
     .when((F.col("anomaly") == "⚠️ ANOMALIE") | (F.col("price_eur_mwh") > 300), "HAUTE")
     .when(F.col("prediction_error_mw") > 1.0, "MOYENNE")
     .otherwise("BASSE")
)

# Cette cellule ne fait que calculer : on affiche la répartition type × priorité
print("✅ Colonnes ajoutées : alert_type, priority")
df_alerts.groupBy("alert_type", "priority").count().orderBy("priority", "alert_type").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Colonnes finales (alert_time = heure de l'alerte) puis tri :
# priorité (CRITIQUE d'abord) puis alerte la plus récente
df_alerts = df_alerts.select(
    F.col("hour").alias("alert_time"),
    "site_id", "site_type", "alert_type",
    "avg_consumption_mw", "baseline_7d_mw", "predicted_consumption_mw",
    "price_eur_mwh", "priority"
).orderBy(
    F.when(F.col("priority") == "CRITIQUE", 1)
     .when(F.col("priority") == "HAUTE",    2)
     .when(F.col("priority") == "MOYENNE",  3)
     .otherwise(4),
    F.desc("alert_time")
)

# Sauvegarde Delta
df_alerts.write.mode("overwrite").format("delta").saveAsTable("gold.active_alerts")
print(f"✅ gold.active_alerts : {df_alerts.count():,} lignes")

# Aperçu : 5 premières alertes CRITIQUE/HAUTE (s'il y en a)
df_alerts.filter("priority IN ('CRITIQUE', 'HAUTE')").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Liste des 6 tables Gold à contrôler
tables_gold = [
    "gold.daily_consumption_by_site",
    "gold.site_kpis",
    "gold.consumption_by_site_type",
    "gold.curtailment_opportunities",
    "gold.monthly_consumption_pivot",
    "gold.active_alerts"
]

print("=" * 55)
print("📦 TABLES GOLD — RÉCAPITULATIF FINAL")
print("=" * 55)
for table in tables_gold:
    # Relit chaque table depuis le lakehouse pour confirmer qu'elle existe
    count = spark.table(f"Lakehouse_gold.{table}").count()
    print(f"  ✅ {table:<40} {count:>7,} lignes")
print("=" * 55)
print("➡️  Prochaine étape : NB_06 Visualisation matplotlib")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
