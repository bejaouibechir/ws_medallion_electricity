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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# --- Imports : matplotlib / seaborn pour les graphiques, pandas / numpy pour les données ---
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import pandas as pd
import numpy as np

# ─── Configuration du style visuel global (appliquée à tous les graphiques) ───
plt.rcParams.update({
    'figure.facecolor': 'white',        # fond de la figure
    'axes.facecolor': '#f8f9fa',        # fond des graphiques (gris très clair)
    'axes.grid': True,                  # grille active
    'grid.alpha': 0.3,                  # grille discrète
    'font.family': 'DejaVu Sans',
    'axes.spines.top': False,           # supprime le cadre haut et droit
    'axes.spines.right': False
})

# ─── Palette de couleurs par site (cohérente sur tous les graphiques) ───
SITE_COLORS = {
    'SITE_IND_001': '#e63946',
    'SITE_IND_002': '#f4a261',
    'SITE_COM_001': '#2a9d8f',
    'SITE_COM_002': '#457b9d',
    'SITE_RES_001': '#8ecae6',
    'SITE_RES_002': '#b7e4c7'
}

# Couleurs par type : les clés doivent correspondre EXACTEMENT aux valeurs de site_type
# dans les tables Gold ("Residentiel", sans accent)
TYPE_COLORS = {
    'Industrie': '#e63946',
    'Commercial': '#2a9d8f',
    'Residentiel': '#8ecae6'
}

# ─── Chargement des tables Gold : Spark -> pandas (nécessaire pour matplotlib) ───
df_daily = spark.table("Lakehouse_gold.gold.daily_consumption_by_site").toPandas()
df_kpis = spark.table("Lakehouse_gold.gold.site_kpis").toPandas()
df_by_type = spark.table("Lakehouse_gold.gold.consumption_by_site_type").toPandas()
df_curtailment = spark.table("Lakehouse_gold.gold.curtailment_opportunities").toPandas()
df_alerts = spark.table("Lakehouse_gold.gold.active_alerts").toPandas()

# ─── Conversions de types pour dates (axes temporels corrects) ───
df_daily['date'] = pd.to_datetime(df_daily['date'])
df_by_type['date'] = pd.to_datetime(df_by_type['date'])
df_curtailment['hour'] = pd.to_datetime(df_curtailment['hour'])
df_alerts['alert_time'] = pd.to_datetime(df_alerts['alert_time'])

print("✅ Données chargées et prêtes")
print(f"  • daily_consumption_by_site  : {len(df_daily):,} lignes")
print(f"  • site_kpis                  : {len(df_kpis)} lignes")
print(f"  • consumption_by_site_type   : {len(df_by_type)} lignes")
print(f"  • curtailment_opportunities  : {len(df_curtailment):,} lignes")
print(f"  • active_alerts              : {len(df_alerts)} lignes")
print(f"\n📊 Palettes de couleurs définies ({len(SITE_COLORS)} sites, {len(TYPE_COLORS)} types)")
print(f"📅 Période couverte : {df_daily['date'].min():%d/%m/%Y} → {df_daily['date'].max():%d/%m/%Y}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3 sous-graphiques empilés (un par type de site), axe X (dates) partagé
fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

# Regroupement des sites par type (un sous-graphique par groupe)
site_groups = {
    'Industrie': ['SITE_IND_001', 'SITE_IND_002'],
    'Commercial': ['SITE_COM_001', 'SITE_COM_002'],
    'Résidentiel': ['SITE_RES_001', 'SITE_RES_002']
}

for ax, (site_type, sites) in zip(axes, site_groups.items()):
    for site in sites:
        # Données du site, triées par date
        data = df_daily[df_daily['site_id'] == site].sort_values('date')
        # Courbe de consommation moyenne journalière + zone colorée sous la courbe
        ax.plot(data['date'], data['avg_consumption_mw'],
                color=SITE_COLORS[site], label=site, linewidth=1.8, alpha=0.9)
        ax.fill_between(data['date'], data['avg_consumption_mw'],
                        alpha=0.08, color=SITE_COLORS[site])

    ax.set_title(f'Sites {site_type}', fontsize=12, fontweight='bold', pad=8)
    ax.set_ylabel('Consommation (MW)', fontsize=10)
    ax.legend(loc='upper right', fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))     # dates au format jj/mm
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))   # une graduation par semaine

axes[-1].set_xlabel('Date', fontsize=10)
fig.suptitle('Évolution de la consommation électrique par site (30 jours)',
             fontsize=14, fontweight='bold', y=1.01)
plt.tight_layout()
plt.savefig('/tmp/viz1_evolution_temporelle.png', dpi=150, bbox_inches='tight')   # export PNG
plt.show()

# Résumé chiffré : consommation moyenne et pic journalier par site
resume = (df_daily.groupby('site_id')
          .agg(moyenne_mw=('avg_consumption_mw', 'mean'), pic_mw=('peak_consumption_mw', 'max'))
          .round(2))
print("✅ Viz 1 générée : évolution temporelle")
print(resume)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3 graphiques côte à côte : taux de charge, taux d'anomalies, erreur de prédiction
fig, axes = plt.subplots(1, 3, figsize=(16, 6))

# ─── KPI 1 : Taux de charge ───
df_kpis_sorted = df_kpis.sort_values('load_factor_pct', ascending=True)
colors = [TYPE_COLORS[t] for t in df_kpis_sorted['site_type']]     # couleur selon le type du site

bars1 = axes[0].barh(df_kpis_sorted['site_id'], df_kpis_sorted['load_factor_pct'],
                      color=colors, edgecolor='white', linewidth=0.5)
axes[0].set_xlabel('Taux de charge (%)', fontsize=10)
axes[0].set_title('Taux de charge\nmoyen mensuel', fontsize=11, fontweight='bold')
axes[0].axvline(x=df_kpis['load_factor_pct'].mean(), color='gray', linestyle='--',
                alpha=0.7, label='Moyenne')                        # ligne de moyenne
for bar, val in zip(bars1, df_kpis_sorted['load_factor_pct']):     # valeur écrite au bout de chaque barre
    axes[0].text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                 f'{val:.1f}%', va='center', fontsize=9)
axes[0].legend(fontsize=9)

# ─── KPI 2 : Taux d'anomalies ───
df_kpis_anom = df_kpis.sort_values('anomaly_rate_pct', ascending=True)
colors_anom = [TYPE_COLORS[t] for t in df_kpis_anom['site_type']]
bars2 = axes[1].barh(df_kpis_anom['site_id'], df_kpis_anom['anomaly_rate_pct'],
                      color=colors_anom, edgecolor='white', linewidth=0.5)
axes[1].set_xlabel("Taux d'anomalies (%)", fontsize=10)
axes[1].set_title("Taux d'anomalies\n(z-score > 3σ)", fontsize=11, fontweight='bold')
for bar, val in zip(bars2, df_kpis_anom['anomaly_rate_pct']):
    axes[1].text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2,
                 f'{val:.2f}%', va='center', fontsize=9)

# ─── KPI 3 : Erreur de prédiction ───
df_kpis_pred = df_kpis.sort_values('prediction_error_pct', ascending=True)
colors_pred = [TYPE_COLORS[t] for t in df_kpis_pred['site_type']]
bars3 = axes[2].barh(df_kpis_pred['site_id'], df_kpis_pred['prediction_error_pct'],
                      color=colors_pred, edgecolor='white', linewidth=0.5)
axes[2].set_xlabel('Erreur de prédiction (%)', fontsize=10)
axes[2].set_title('Erreur de prédiction\nmoyenne mensuelle', fontsize=11, fontweight='bold')
for bar, val in zip(bars3, df_kpis_pred['prediction_error_pct']):
    axes[2].text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                 f'{val:.1f}%', va='center', fontsize=9)

# ─── Légende commune (types de sites) ───
from matplotlib.patches import Patch
legend_elements = [Patch(facecolor=c, label=t) for t, c in TYPE_COLORS.items()]
fig.legend(handles=legend_elements, loc='lower center', ncol=3, fontsize=10,
           bbox_to_anchor=(0.5, -0.05))

fig.suptitle('KPIs comparatifs — 6 sites électriques (30 jours)',
             fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('/tmp/viz2_kpis_comparatifs.png', dpi=150, bbox_inches='tight')
plt.show()

# Résumé chiffré : site extrême pour chaque KPI
print("✅ Viz 2 générée : KPIs comparatifs")
print(f"  • Taux de charge max     : {df_kpis.loc[df_kpis['load_factor_pct'].idxmax(), 'site_id']} ({df_kpis['load_factor_pct'].max():.1f} %)")
print(f"  • Taux d'anomalies max   : {df_kpis.loc[df_kpis['anomaly_rate_pct'].idxmax(), 'site_id']} ({df_kpis['anomaly_rate_pct'].max():.2f} %)")
print(f"  • Erreur de prédiction max : {df_kpis.loc[df_kpis['prediction_error_pct'].idxmax(), 'site_id']} ({df_kpis['prediction_error_pct'].max():.1f} %)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2 graphiques côte à côte : (gauche) prix vs consommation, (droite) gains d'effacement
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# ─── Scatter : Prix vs Consommation par type (1 point = 1 site × 1 jour) ───
for site_type, color in TYPE_COLORS.items():
    mask = df_daily['site_type'] == site_type
    ax1.scatter(df_daily[mask]['avg_price_eur_mwh'],
                df_daily[mask]['avg_consumption_mw'],
                c=color, label=site_type, alpha=0.6, s=40,
                edgecolors='white', linewidth=0.3)

# Ligne de tendance globale (régression linéaire de degré 1 : y = a·x + b)
z = np.polyfit(df_daily['avg_price_eur_mwh'].fillna(0),
               df_daily['avg_consumption_mw'].fillna(0), 1)
p = np.poly1d(z)
x_range = np.linspace(df_daily['avg_price_eur_mwh'].min(),
                      df_daily['avg_price_eur_mwh'].max(), 100)
ax1.plot(x_range, p(x_range), "k--", alpha=0.4, linewidth=1.5, label='Tendance')

# Seuils de prix pour effacement (les mêmes que dans l'atelier Gold)
ax1.axvline(x=200, color='orange', linestyle=':', alpha=0.7, label='Seuil effacement partiel')
ax1.axvline(x=300, color='red', linestyle=':', alpha=0.7, label='Seuil effacement max')
ax1.set_xlabel('Prix spot moyen journalier (€/MWh)', fontsize=10)
ax1.set_ylabel('Consommation moyenne journalière (MW)', fontsize=10)
ax1.set_title('Corrélation Prix marché ↔ Consommation', fontsize=12, fontweight='bold')
ax1.legend(fontsize=9)
ax1.grid(True, alpha=0.3)

# ─── Potentiel d'effacement par site (gains cumulés des heures rentables) ───
curtail_gains = df_curtailment[df_curtailment['potential_gain_eur'] > 0].groupby('site_id')['potential_gain_eur'].sum().reset_index()
curtail_gains = curtail_gains.sort_values('potential_gain_eur', ascending=False)
colors_c = [SITE_COLORS[s] for s in curtail_gains['site_id']]

bars = ax2.bar(curtail_gains['site_id'], curtail_gains['potential_gain_eur'],
               color=colors_c, edgecolor='white', linewidth=0.5)
ax2.set_xlabel('Site', fontsize=10)
ax2.set_ylabel('Gain potentiel cumulé (€)', fontsize=10)
ax2.set_title("Gains potentiels d'effacement sur 30 jours", fontsize=12, fontweight='bold')
ax2.tick_params(axis='x', rotation=30)
for bar, val in zip(bars, curtail_gains['potential_gain_eur']):    # montant au-dessus de chaque barre
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
             f'{val:,.0f} €', ha='center', fontsize=9, fontweight='bold')

plt.suptitle("Marchés électriques : sensibilité au prix et optimisation d'effacement",
             fontsize=13, fontweight='bold')
plt.tight_layout()
plt.savefig('/tmp/viz3_prix_effacement.png', dpi=150, bbox_inches='tight')
plt.show()

# Résumé chiffré : pente de la tendance et gains
print("✅ Viz 3 générée : prix et effacement")
print(f"  • Pente de la tendance : {z[0]:+.4f} MW par €/MWh ({'consommation qui baisse' if z[0] < 0 else 'consommation qui monte'} quand le prix monte)")
print(f"  • Prix spot moyen journalier : de {df_daily['avg_price_eur_mwh'].min():.0f} à {df_daily['avg_price_eur_mwh'].max():.0f} €/MWh (seuils 200 / 300 €/MWh)")
print(f"  • Gain d'effacement cumulé : {curtail_gains['potential_gain_eur'].sum():,.0f} € sur {len(curtail_gains)} sites")
print(curtail_gains.round(0).to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Charge les données HORAIRES depuis Silver (Gold est trop agrégée pour une heatmap heure × jour)
df_hourly = spark.table("Lakehouse_silver.silver.consumption_with_prices").toPandas()
df_hourly['hour'] = pd.to_datetime(df_hourly['hour'])
df_hourly['hour_of_day'] = df_hourly['hour'].dt.hour             # 0 à 23
df_hourly['day_of_week'] = df_hourly['hour'].dt.day_name()       # nom du jour (anglais)
print(f"📊 Données horaires chargées : {len(df_hourly):,} lignes")

# Agrégation par jour de semaine × heure (moyenne tous sites confondus)
day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
day_labels_fr = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']

pivot = df_hourly.groupby(['day_of_week', 'hour_of_day'])['avg_consumption_mw'].mean().unstack()
pivot = pivot.reindex(day_order)                                 # ordre Lundi -> Dimanche

fig, ax = plt.subplots(figsize=(14, 6))
sns.heatmap(pivot, ax=ax,
            cmap='YlOrRd',                  # jaune (faible) -> rouge (fort)
            fmt='.2f',
            annot=False,
            linewidths=0.3,
            linecolor='white',
            cbar_kws={'label': 'Consommation moyenne (MW)'})

ax.set_xticklabels([f'{h}h' for h in range(24)], fontsize=8, rotation=0)
ax.set_yticklabels(day_labels_fr, fontsize=10, rotation=0)
ax.set_xlabel('Heure de la journée', fontsize=11)
ax.set_ylabel('Jour de la semaine', fontsize=11)
ax.set_title('Patterns de consommation : Heure × Jour de la semaine\n(moyenne tous sites)',
             fontsize=13, fontweight='bold', pad=28)

# Repères : début et fin d'activité (8 h et 19 h)
ax.axvline(x=8, color='navy', linewidth=2, alpha=0.5, linestyle='--')
ax.axvline(x=19, color='navy', linewidth=2, alpha=0.5, linestyle='--')
ax.text(8.2, -0.5, 'Début activité', fontsize=8, color='navy', ha='left')
ax.text(19.2, -0.5, 'Fin activité', fontsize=8, color='navy', ha='left')

plt.tight_layout()
plt.savefig('/tmp/viz4_heatmap_temporelle.png', dpi=150, bbox_inches='tight')
plt.show()

# Résumé chiffré : heure la plus / moins chargée, semaine vs week-end
moy_heure = pivot.mean(axis=0)
semaine = pivot.loc[day_order[:5]].values.mean()
weekend = pivot.loc[day_order[5:]].values.mean()
print("✅ Viz 4 générée : heatmap temporelle")
print(f"  • Heure la plus chargée  : {moy_heure.idxmax()} h ({moy_heure.max():.2f} MW)")
print(f"  • Heure la moins chargée : {moy_heure.idxmin()} h ({moy_heure.min():.2f} MW)")
print(f"  • Semaine : {semaine:.2f} MW | Week-end : {weekend:.2f} MW")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2 graphiques côte à côte : (gauche) répartition des alertes, (droite) timeline des alertes
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# ─── Distribution des alertes par type et priorité ───
priority_colors = {
    'CRITIQUE': '#e63946',
    'HAUTE': '#f4a261',
    'MOYENNE': '#2a9d8f',
    'BASSE': '#8ecae6'
}

# Comptage type × priorité, puis pivot (types en lignes, priorités en colonnes)
alert_counts = df_alerts.groupby(['alert_type', 'priority']).size().reset_index(name='count')
pivot_alerts = alert_counts.pivot(index='alert_type', columns='priority', values='count').fillna(0)

# Réordonner les colonnes prioritaires (seules celles présentes dans les données)
priority_order = ['CRITIQUE', 'HAUTE', 'MOYENNE', 'BASSE']
pivot_alerts = pivot_alerts[[col for col in priority_order if col in pivot_alerts.columns]]

pivot_alerts.plot(kind='bar', ax=ax1,
                  color=[priority_colors.get(c, '#808080') for c in pivot_alerts.columns],
                  edgecolor='white', linewidth=0.5)
ax1.set_xlabel('Type d\'alerte', fontsize=10)
ax1.set_ylabel('Nombre d\'alertes', fontsize=10)
ax1.set_title('Distribution des alertes par type et priorité', fontsize=12, fontweight='bold')
ax1.tick_params(axis='x', rotation=20)
ax1.legend(title='Priorité', fontsize=9)
ax1.grid(axis='y', alpha=0.3)

# ─── Timeline des alertes CRITIQUE et HAUTE (date en X, prix spot en Y) ───
df_high = df_alerts[df_alerts['priority'].isin(['CRITIQUE', 'HAUTE'])].copy()
df_high['alert_time'] = pd.to_datetime(df_high['alert_time'])

for priority, color in [('CRITIQUE', '#e63946'), ('HAUTE', '#f4a261')]:
    subset = df_high[df_high['priority'] == priority]
    if len(subset) == 0:                       # aucune alerte de cette priorité : on passe
        print(f"ℹ️ Aucune alerte {priority} dans les données")
        continue
    ax2.scatter(subset['alert_time'], subset['price_eur_mwh'],
                c=color, label=priority, s=60, alpha=0.8,
                edgecolors='white', linewidth=0.3)

ax2.axhline(y=300, color='red', linestyle='--', alpha=0.5, linewidth=1,
            label='Seuil prix 300€/MWh')
ax2.set_xlabel('Date/heure', fontsize=10)
ax2.set_ylabel('Prix spot (€/MWh)', fontsize=10)
ax2.set_title('Timeline des alertes critiques\n(corrélation avec pic de prix)',
              fontsize=12, fontweight='bold')
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
ax2.tick_params(axis='x', rotation=30, labelsize=8)
ax2.legend(fontsize=9)
ax2.grid(True, alpha=0.3)

plt.suptitle('Tableau de bord opérationnel — Alertes et anomalies',
             fontsize=13, fontweight='bold')
plt.tight_layout()
plt.savefig('/tmp/viz5_alertes.png', dpi=150, bbox_inches='tight')
plt.show()

# Résumé chiffré : répartition par priorité et site le plus touché
print("✅ Viz 5 générée : alertes et anomalies")
print(f"  • Total alertes : {len(df_alerts)}")
print("  • Par priorité :", df_alerts['priority'].value_counts().to_dict())
print(f"  • Site le plus touché : {df_alerts['site_id'].value_counts().idxmax()} ({df_alerts['site_id'].value_counts().max()} alertes)")
print(f"  • Alertes avec prix > 300 €/MWh : {(df_alerts['price_eur_mwh'] > 300).sum()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Page unique au format paysage, 4 panneaux (2 × 2)
fig = plt.figure(figsize=(16, 10))
fig.suptitle('Tableau de Bord Énergétique — Synthèse Mensuelle\nParc de 6 Sites | Janvier 2025',
             fontsize=15, fontweight='bold', y=0.98)

# ─── Layout 2×2 : 4 zones de dessin ───
ax1 = fig.add_subplot(2, 2, 1)
ax2 = fig.add_subplot(2, 2, 2)
ax3 = fig.add_subplot(2, 2, 3)
ax4 = fig.add_subplot(2, 2, 4)

# ──── Mini-graphique 1 : Consommation totale par type (donut) ────
# Consommation mensuelle cumulée par type de site
total_by_type = df_kpis.groupby('site_type')['total_consumption_month_mwh'].sum()
# Séparateurs blancs entre parts + léger décalage de chaque part
wedge_props = {'linewidth': 2, 'edgecolor': 'white'}
explode = (0.05, 0.05, 0.05)

ax1.pie(total_by_type.values,
        labels=[f'{t}\n{v:.0f} MWh' for t, v in zip(total_by_type.index, total_by_type.values)],
        colors=[TYPE_COLORS[t] for t in total_by_type.index],
        autopct='%1.1f%%', startangle=90,
        wedgeprops=wedge_props, textprops={'fontsize': 9},
        explode=explode)
ax1.set_title('Répartition de la consommation\npar type de site',
              fontsize=11, fontweight='bold', pad=10)

# ──── Mini-graphique 2 : Évolution du prix marché ────
# Prix moyen journalier (moyenne des 6 sites), trié par date
df_price_daily = df_daily.groupby('date')['avg_price_eur_mwh'].mean().reset_index()
df_price_daily = df_price_daily.sort_values('date')

ax2.plot(df_price_daily['date'], df_price_daily['avg_price_eur_mwh'],
         color='#e63946', linewidth=2, marker='o', markersize=3)
ax2.fill_between(df_price_daily['date'], df_price_daily['avg_price_eur_mwh'],
                 alpha=0.1, color='#e63946')
ax2.axhline(y=200, color='orange', linestyle='--', alpha=0.6, linewidth=1.5,
            label='Seuil effacement')
ax2.set_ylabel('€/MWh', fontsize=9)
ax2.set_xlabel('Date', fontsize=9)
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
ax2.set_title('Évolution du prix spot EPEX', fontsize=11, fontweight='bold')
ax2.legend(fontsize=8)
ax2.tick_params(axis='x', rotation=30, labelsize=7)
ax2.grid(True, alpha=0.3)

# ──── Mini-graphique 3 : Load factor par site (barres) ────
# Sites triés du plus au moins chargé
df_lf = df_kpis.sort_values('load_factor_pct', ascending=False)
colors_lf = [TYPE_COLORS[t] for t in df_lf['site_type']]

bars_lf = ax3.bar(range(len(df_lf)), df_lf['load_factor_pct'],
                   color=colors_lf, edgecolor='white', linewidth=0.5)
ax3.set_xticks(range(len(df_lf)))
ax3.set_xticklabels(df_lf['site_id'], rotation=30, ha='right', fontsize=8)
ax3.set_ylabel('Taux de charge (%)', fontsize=9)
ax3.set_title('Taux de charge mensuel\npar site', fontsize=11, fontweight='bold')

# Ligne de moyenne du taux de charge
avg_lf = df_kpis['load_factor_pct'].mean()
ax3.axhline(y=avg_lf, color='gray', linestyle='--', alpha=0.7, linewidth=1.5,
            label=f'Moyenne {avg_lf:.1f}%')
ax3.legend(fontsize=8)
ax3.grid(axis='y', alpha=0.3)

for bar, val in zip(bars_lf, df_lf['load_factor_pct']):
    ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
             f'{val:.1f}%', ha='center', fontsize=8, fontweight='bold')

# ──── Mini-graphique 4 : Gains effacement cumulés par site ────
# Gains cumulés des heures rentables, par site
gains = df_curtailment[df_curtailment['potential_gain_eur'] > 0].groupby('site_id')['potential_gain_eur'].sum().reset_index()
gains = gains.sort_values('potential_gain_eur', ascending=False)
colors_g = [SITE_COLORS[s] for s in gains['site_id']]

bars_g = ax4.bar(gains['site_id'], gains['potential_gain_eur'],
                  color=colors_g, edgecolor='white', linewidth=0.5)
ax4.set_ylabel('Gain potentiel (€)', fontsize=9)
ax4.set_xlabel('Site', fontsize=9)
ax4.set_title("Gains d'effacement potentiels\n(sites flexibles)", fontsize=11, fontweight='bold')
ax4.tick_params(axis='x', rotation=30, labelsize=8)
ax4.grid(axis='y', alpha=0.3)

# Encadré avec le gain total
total_gain = gains['potential_gain_eur'].sum()
ax4.text(0.98, 0.95, f'Total : {total_gain:,.0f} €',
         transform=ax4.transAxes, ha='right', fontsize=10, fontweight='bold',
         bbox=dict(boxstyle='round', facecolor='#f4a261', alpha=0.3, pad=0.5))

for bar, val in zip(bars_g, gains['potential_gain_eur']):
    ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + total_gain*0.02,
             f'{val:,.0f}€', ha='center', fontsize=8, fontweight='bold')

plt.tight_layout(rect=[0, 0, 1, 0.96])   # laisse la place au titre général
plt.savefig('/tmp/viz6_dashboard_synthese.png', dpi=150, bbox_inches='tight')
plt.show()
print("✅ Dashboard de synthèse généré")
print(f"\n📊 6 visualisations créées — prêtes pour intégration dans rapports et présentations")
print(f"  • Consommation totale du parc : {total_by_type.sum():,.0f} MWh")
print(f"  • Prix spot moyen             : {df_price_daily['avg_price_eur_mwh'].mean():.1f} €/MWh")
print(f"  • Taux de charge moyen        : {avg_lf:.1f} %")
print(f"  • Gain d'effacement potentiel : {total_gain:,.0f} €")
print("  • PNG exportés dans /tmp : viz1 … viz6")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
