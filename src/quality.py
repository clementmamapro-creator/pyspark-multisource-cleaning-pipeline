# =====================================================================
# quality.py
# Partie 3  : audit initial de la qualite (rapport par source/colonne)
# Partie 12 : controles de coherence sur customer_order_360
# Partie 13 : consolidation et ecriture de la zone de rejet
# =====================================================================

from pyspark.sql.functions import col, current_timestamp, to_json, struct, lit as spark_lit


# ---------------------------------------------------------------------
# PARTIE 3 : AUDIT INITIAL DE LA QUALITE
# ---------------------------------------------------------------------

def audit_qualite(spark, df, nom_source):
    """
    Calcule, pour chaque colonne d'un DataFrame : nombre de lignes,
    nombre de nulles, pourcentage de nulles, nombre de chaines vides.
    Retourne un DataFrame de synthese avec la structure imposee par
    le sujet : source | colonne | nombre_lignes | nombre_nulles |
    pourcentage_nulles | nombre_invalides.
    """
    nb_lignes = df.count()
    resultats = []

    for colonne in df.columns:
        nb_nulles = df.filter(col(colonne).isNull()).count()
        pct_nulles = round((nb_nulles / nb_lignes) * 100, 2) if nb_lignes > 0 else 0
        nb_vides = df.filter(col(colonne) == "").count() if dict(df.dtypes)[colonne] == "string" else 0
        resultats.append((nom_source, colonne, nb_lignes, nb_nulles, pct_nulles, nb_vides))

    colonnes_rapport = ["source", "colonne", "nombre_lignes", "nombre_nulles", "pourcentage_nulles", "nombre_invalides"]
    return spark.createDataFrame(resultats, colonnes_rapport)


def build_data_quality_report(spark, sources: dict):
    """
    Etape 5 : applique audit_qualite() a chaque source du dictionnaire
    {nom_source: dataframe}, puis fusionne le tout en un seul rapport.
    """
    rapports = [audit_qualite(spark, df, nom) for nom, df in sources.items()]
    rapport_qualite = rapports[0]
    for r in rapports[1:]:
        rapport_qualite = rapport_qualite.unionByName(r)
    return rapport_qualite


def write_data_quality_report(rapport_qualite, output_path):
    """Ecrit le rapport de qualite en Parquet (1 seul fichier)."""
    rapport_qualite.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"Rapport de qualite ecrit dans {output_path}")


# ---------------------------------------------------------------------
# PARTIE 12 : CONTROLES DE COHERENCE
# ---------------------------------------------------------------------

def run_validation_controls(spark, customer_order_360):
    """
    Etape 35 : effectue les 8 controles demandes par le sujet et
    retourne un DataFrame validation_results (controle | nombre_anomalies | statut).
    """
    resultats = []

    nb_doublons_id = customer_order_360.groupBy("order_id").count().filter(col("count") > 1).count()
    resultats.append(("Unicité des commandes", nb_doublons_id, "OK" if nb_doublons_id == 0 else "ERREUR"))

    nb_montants_negatifs = customer_order_360.filter(col("calculated_amount_eur") < 0).count()
    resultats.append(("Montants négatifs", nb_montants_negatifs, "OK" if nb_montants_negatifs == 0 else "ERREUR"))

    nb_notes_invalides = customer_order_360.filter(
        col("average_rating").isNotNull() & ((col("average_rating") < 1) | (col("average_rating") > 5))
    ).count()
    resultats.append(("Notes moyennes hors intervalle", nb_notes_invalides, "OK" if nb_notes_invalides == 0 else "ERREUR"))

    nb_scores_invalides = customer_order_360.filter(
        (col("data_quality_score") < 0) | (col("data_quality_score") > 100)
    ).count()
    resultats.append(("Scores qualité hors intervalle", nb_scores_invalides, "OK" if nb_scores_invalides == 0 else "ERREUR"))

    nb_livraison_avant_commande = customer_order_360.filter(
        col("delivery_date").isNotNull() & col("order_date").isNotNull() & (col("delivery_date") < col("order_date"))
    ).count()
    resultats.append(("Livraison avant commande", nb_livraison_avant_commande, "OK" if nb_livraison_avant_commande == 0 else "ERREUR"))

    nb_annulees_livrees = customer_order_360.filter(
        (col("order_status") == "CANCELLED") & (col("last_delivery_status") == "DELIVERED")
    ).count()
    resultats.append(("Commandes annulées mais livrées", nb_annulees_livrees, "OK" if nb_annulees_livrees == 0 else "ERREUR"))

    nb_livrees_sans_date = customer_order_360.filter(
        (col("last_delivery_status") == "DELIVERED") & col("delivery_date").isNull()
    ).count()
    resultats.append(("Livrées sans date de livraison", nb_livrees_sans_date, "OK" if nb_livrees_sans_date == 0 else "ERREUR"))

    nb_statuts_manquants = customer_order_360.filter(col("order_status").isNull()).count()
    resultats.append(("Statuts manquants", nb_statuts_manquants, "OK" if nb_statuts_manquants == 0 else "ERREUR"))

    validation_results = spark.createDataFrame(resultats, ["controle", "nombre_anomalies", "statut"])
    return validation_results


# ---------------------------------------------------------------------
# PARTIE 13 : GESTION DES REJETS
# ---------------------------------------------------------------------

def creer_rejet(df, condition, motif, nom_source):
    """
    Isole les lignes rejetees d'un DataFrame et les reformate selon la
    structure commune imposee par le sujet : source, rejection_reason,
    rejection_timestamp, original_data (JSON complet de la ligne).
    """
    lignes_rejetees = df.filter(condition)
    return lignes_rejetees.select(
        spark_lit(nom_source).alias("source"),
        spark_lit(motif).alias("rejection_reason"),
        current_timestamp().alias("rejection_timestamp"),
        to_json(struct([df[c] for c in df.columns])).alias("original_data"),
    )


def write_rejects(rejects: dict, output_dir):
    """
    Ecrit chaque DataFrame de rejet dans son sous-dossier dedie :
    rejects/customers, rejects/orders, rejects/order_items,
    rejects/reviews, rejects/delivery_events.
    """
    for nom_source, df_rejet in rejects.items():
        chemin = f"{output_dir}/{nom_source}"
        df_rejet.coalesce(1).write.mode("overwrite").parquet(chemin)
        print(f"Rejets '{nom_source}' ecrits dans {chemin} ({df_rejet.count()} lignes)")
