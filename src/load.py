# =====================================================================
# load.py
# Partie 14 : ecriture finale partitionnee + rechargement/verification.
# Partie 15 : execution des requetes analytiques (sql/analytical_queries.sql).
# Partie 16 : demonstrations d'optimisation (explain, broadcast, cache,
#             repartition/coalesce) - executees uniquement si demandees
#             explicitement, pour ne pas alourdir une execution normale.
# =====================================================================

import time
from pyspark.sql.functions import col, broadcast


# ---------------------------------------------------------------------
# PARTIE 14 : ECRITURE FINALE
# ---------------------------------------------------------------------

def write_customer_order_360(customer_order_360, output_path):
    """
    Etape 37 : ecrit customer_order_360 en Parquet, partitionne par
    order_year/order_month. Le repartition() prealable regroupe les
    lignes par partition de sortie pour eviter la multiplication de
    petits fichiers (voir Partie 16, Etape 44, pour la demonstration
    chiffree de cet effet).
    """
    df_final = customer_order_360.repartition("order_year", "order_month")
    df_final.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(output_path)
    print(f"customer_order_360 ecrit dans {output_path} (partitionne par order_year/order_month)")


def reload_and_verify(spark, output_path, nb_lignes_avant):
    """
    Etape 38 : recharge le Parquet ecrit et verifie schema, nombre de
    lignes, unicite de order_id, nombre de partitions, annees/mois
    disponibles, et presence de toutes les colonnes.
    """
    df_reload = spark.read.parquet(output_path)

    print("\n=== Etape 38 : verification apres rechargement ===")
    df_reload.printSchema()

    nb_lignes_apres = df_reload.count()
    print("Lignes avant ecriture   :", nb_lignes_avant)
    print("Lignes apres rechargement:", nb_lignes_apres)
    if nb_lignes_avant != nb_lignes_apres:
        print("ALERTE : le nombre de lignes a change apres ecriture/rechargement !")

    nb_doublons = df_reload.groupBy("order_id").count().filter(col("count") > 1).count()
    print("Doublons order_id apres rechargement :", nb_doublons)

    print("Nombre de partitions du DataFrame :", df_reload.rdd.getNumPartitions())

    print("--- Annees et mois disponibles ---")
    df_reload.select("order_year", "order_month").distinct().orderBy("order_year", "order_month").show()

    return df_reload


# ---------------------------------------------------------------------
# PARTIE 15 : ANALYSES SPARK SQL
# ---------------------------------------------------------------------

def create_temp_view(customer_order_360):
    """Etape 39 : cree la vue temporaire customer_order_360_view."""
    customer_order_360.createOrReplaceTempView("customer_order_360_view")
    print("Vue temporaire creee : customer_order_360_view")


def run_analytical_queries(spark, sql_file_path="sql/analytical_queries.sql"):
    """
    Etape 40 : lit le fichier sql/analytical_queries.sql, decoupe les
    10 requetes sur le marqueur "-- ###" et les execute une a une.
    Necessite que create_temp_view() ait deja ete appelee.
    """
    with open(sql_file_path, "r", encoding="utf-8") as f:
        contenu = f.read()

    # On ignore le tout premier segment : c'est l'en-tete du fichier,
    # situe AVANT le premier vrai marqueur "-- ###", jamais une requete.
    segments = contenu.split("-- ###")[1:]
    blocs = [b.strip() for b in segments if b.strip()]

    for bloc in blocs:
        lignes = bloc.splitlines()
        titre = lignes[0].strip()
        requete = "\n".join(lignes[1:])
        print(f"\n=== {titre} ===")
        spark.sql(requete).show(20, truncate=False)


# ---------------------------------------------------------------------
# PARTIE 16 : OPTIMISATION (demonstrations, a lancer a la demande)
# ---------------------------------------------------------------------

def demo_explain_plans(orders_clean, customers_clean, taux_change_df):
    """Etape 41 : affiche les plans d'execution de 2 jointures differentes."""
    print("\n=== Plan d'execution : orders_clean JOIN customers_clean (SortMergeJoin attendu) ===")
    orders_clean.join(
        customers_clean.select("customer_id_clean", "full_name"), "customer_id_clean", "left"
    ).explain("formatted")

    print("\n=== Plan d'execution : orders_clean JOIN taux_change broadcast (BroadcastHashJoin attendu) ===")
    orders_clean.select("currency_clean", "total_amount_clean").join(
        broadcast(taux_change_df), "currency_clean", "left"
    ).explain("formatted")


def demo_cache(customer_order_360):
    """Etape 43 : compare le temps d'execution sans cache / avec cache / apres unpersist()."""
    df = customer_order_360

    debut = time.time()
    df.count()
    df.filter(col("is_amount_consistent") == False).count()
    df.groupBy("order_status").count().collect()
    print(f"Temps SANS cache : {time.time() - debut:.2f} sec")

    df.cache()
    df.count()
    debut = time.time()
    df.count()
    df.filter(col("is_amount_consistent") == False).count()
    df.groupBy("order_status").count().collect()
    print(f"Temps AVEC cache : {time.time() - debut:.2f} sec")

    df.unpersist()
    debut = time.time()
    df.count()
    df.filter(col("is_amount_consistent") == False).count()
    df.groupBy("order_status").count().collect()
    print(f"Temps APRES unpersist() : {time.time() - debut:.2f} sec")

    print(
        "Note : sur un petit volume et un cluster a un seul worker, le cache "
        "peut etre plus lent que l'absence de cache (cout de mise en cache "
        "superieur au gain), comme observe pendant le developpement de ce projet."
    )


def demo_repartition_vs_coalesce(customer_order_360, output_dir):
    """Etape 44 : compare repartition(n) et coalesce(n) sur nombre/taille de fichiers et temps."""
    print("Nombre de partitions avant traitement :", customer_order_360.rdd.getNumPartitions())

    df_repartition = customer_order_360.repartition(4)
    debut = time.time()
    df_repartition.write.mode("overwrite").parquet(f"{output_dir}/demo_repartition")
    print(f"repartition(4) -> {df_repartition.rdd.getNumPartitions()} partitions, "
          f"ecriture en {time.time() - debut:.2f} sec")

    df_coalesce = customer_order_360.coalesce(1)
    debut = time.time()
    df_coalesce.write.mode("overwrite").parquet(f"{output_dir}/demo_coalesce")
    print(f"coalesce(1) -> {df_coalesce.rdd.getNumPartitions()} partition, "
          f"ecriture en {time.time() - debut:.2f} sec")
