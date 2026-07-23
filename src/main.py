#!/usr/bin/env python3
# =====================================================================
# main.py
# Point d'entree du pipeline TP Multisource.
# Orchestre les Parties 1 a 16 en appelant les fonctions des modules
# extract.py, cleaning.py, transformations.py, quality.py, load.py.
#
# Lancement :
#   export POSTGRES_PASSWORD=spark_password
#   spark-submit \
#       --master local[*] \
#       --packages org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
#       src/main.py
#
# Le flag --packages est deja repris dans extract.create_spark_session()
# via spark.jars.packages : le passer aussi en ligne de commande evite
# simplement un premier telechargement au demarrage du driver.
# =====================================================================

import os
import sys

# Permet d'executer "python src/main.py" ou "spark-submit src/main.py"
# depuis la racine du projet sans configuration supplementaire.
sys.path.insert(0, os.path.dirname(__file__))

import extract
import cleaning
import transformations
import quality
import load


def main():
    config = extract.load_config("config/application.conf")
    spark = extract.create_spark_session(config)

    # ---------------- PARTIE 2 : CHARGEMENT DES DONNEES ----------------
    df_customers = extract.extract_customers(spark, config)
    df_orders = extract.extract_orders(spark, config)
    df_order_items = extract.extract_order_items(spark, config)
    df_products = extract.extract_products(spark, config)
    df_reviews = extract.extract_reviews(spark, config)
    df_delivery_events = extract.extract_delivery_events(spark, config)

    # ---------------- PARTIE 3 : AUDIT INITIAL DE LA QUALITE ----------------
    rapport_qualite = quality.build_data_quality_report(spark, {
        "customers": df_customers,
        "orders": df_orders,
        "order_items": df_order_items,
        "products": df_products,
        "reviews": df_reviews,
    })
    quality.write_data_quality_report(rapport_qualite, config.get("paths", "data_quality_report_dir"))

    # ---------------- PARTIES 4 A 9 : NETTOYAGE ----------------
    customers_clean = cleaning.clean_customers(df_customers)

    taux_change = {devise: float(taux) for devise, taux in config.items("taux_change")}
    # configparser met les cles en minuscules par defaut : on les remet en majuscules
    taux_change = {devise.upper(): taux for devise, taux in taux_change.items()}
    orders_clean, orders_rejects = cleaning.clean_orders(df_orders, taux_change)

    order_items_clean, order_items_summary, order_items_rejects = cleaning.clean_order_items(df_order_items)
    products_clean = cleaning.clean_products(df_products)
    reviews_clean, reviews_rejects = cleaning.clean_reviews(df_reviews, customers_clean, orders_clean, order_items_clean)
    delivery_events_clean, delivery_summary, delivery_rejects = cleaning.clean_delivery_events(
        df_delivery_events, orders_clean
    )

    # ---------------- PARTIE 13 : ZONE DE REJET ----------------
    customers_rejects = quality.creer_rejet(
        customers_clean, col_is_null_condition(customers_clean, "customer_id_clean"),
        "Identifiant client non normalisable", "customers"
    )
    rejects = {
        "customers": customers_rejects,
        "orders": quality.creer_rejet(orders_rejects, orders_rejects["order_id_clean"].isNotNull(),
                                       "Date de commande non convertible", "orders"),
        "order_items": quality.creer_rejet(order_items_rejects, order_items_rejects["order_id_clean"].isNotNull(),
                                            "Quantité, prix ou remise invalide", "order_items"),
        "reviews": quality.creer_rejet(reviews_rejects, reviews_rejects["order_id_clean"].isNotNull(),
                                        "Note hors intervalle 1-5", "reviews"),
        "delivery_events": quality.creer_rejet(delivery_rejects, delivery_rejects["order_id_clean"].isNotNull(),
                                                "Statut d'événement inconnu", "delivery_events"),
    }
    quality.write_rejects(rejects, config.get("paths", "rejects_dir"))

    # ---------------- PARTIE 10 : CROISEMENT DES SOURCES ----------------
    orders_with_delivery = transformations.cross_all_sources(
        orders_clean, customers_clean, order_items_clean, order_items_summary,
        products_clean, reviews_clean, delivery_summary,
    )

    # ---------------- PARTIE 11 : VUE CONSOLIDEE ----------------
    customer_order_360 = transformations.build_customer_order_360(orders_with_delivery)
    customer_order_360 = transformations.add_quality_score(customer_order_360)
    nb_lignes_avant = customer_order_360.count()
    print("\ncustomer_order_360 : ", nb_lignes_avant, "lignes")

    # ---------------- PARTIE 12 : CONTROLES DE COHERENCE ----------------
    validation_results = quality.run_validation_controls(spark, customer_order_360)
    print("\n=== Partie 12 : validation_results ===")
    validation_results.show(truncate=False)

    # ---------------- PARTIE 14 : ECRITURE FINALE ----------------
    output_path = config.get("paths", "customer_order_360_dir")
    load.write_customer_order_360(customer_order_360, output_path)
    load.reload_and_verify(spark, output_path, nb_lignes_avant)

    # ---------------- PARTIE 15 : ANALYSES SPARK SQL ----------------
    load.create_temp_view(customer_order_360)
    load.run_analytical_queries(spark, "sql/analytical_queries.sql")

    # ---------------- PARTIE 16 : OPTIMISATION (a la demande) ----------------
    if os.environ.get("RUN_OPTIMISATION_DEMO", "false").lower() == "true":
        print("\n=== Partie 16 : demonstrations d'optimisation ===")
        taux_rows = [(devise, taux) for devise, taux in taux_change.items()]
        taux_change_df = spark.createDataFrame(taux_rows, ["currency_clean", "taux"])
        load.demo_explain_plans(orders_clean, customers_clean, taux_change_df)
        load.demo_cache(customer_order_360)
        load.demo_repartition_vs_coalesce(customer_order_360, config.get("paths", "output_dir"))

    spark.stop()
    print("\nPipeline termine avec succes.")


def col_is_null_condition(df, colonne):
    """Petit helper : condition 'colonne est nulle', utilisee pour customers_rejects."""
    return df[colonne].isNull()


if __name__ == "__main__":
    main()
