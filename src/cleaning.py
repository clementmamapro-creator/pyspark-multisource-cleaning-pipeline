# =====================================================================
# cleaning.py
# Parties 4 a 9 : nettoyage individuel de chaque source.
# Chaque fonction retourne un tuple (df_clean, df_rejects) quand des
# rejets sont pertinents pour cette source, afin que quality.py
# puisse ecrire la zone de rejet (Partie 13) sans recalculer la logique.
# =====================================================================

from pyspark.sql import Window
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import (
    col, when, lit, upper, lower, trim as spark_trim, regexp_replace,
    lpad, concat, initcap, concat_ws, to_date, to_timestamp, coalesce,
    current_date, current_timestamp, floor, datediff, row_number, desc,
    length, countDistinct, collect_set, avg as spark_avg, min as spark_min,
    max as spark_max, count as spark_count, round as spark_round,
    abs as spark_abs, broadcast, year, month, sum as spark_sum,
)


# ---------------------------------------------------------------------
# PARTIE 4 : NETTOYAGE DES CLIENTS
# ---------------------------------------------------------------------

def clean_customers(df_customers):
    """
    Etapes 6 a 12 : normalisation complete des clients + deduplication.
    Retourne customers_clean (une seule ligne par customer_id_clean).
    """
    df = df_customers

    # Etape 6 : customer_id -> format C000001
    df = df.withColumn(
        "customer_id_clean",
        upper(spark_trim(col("customer_id")))
    )
    df = df.withColumn(
        "customer_id_clean",
        regexp_replace(
            regexp_replace(col("customer_id_clean"), "[-_ ]", ""),
            "^CUST", "C"
        )
    )
    df = df.withColumn(
        "customer_id_clean",
        concat(lit("C"), lpad(regexp_replace(col("customer_id_clean"), "[^0-9]", ""), 6, "0"))
    )

    # Etape 7 : noms nettoyes + full_name
    df = df.withColumn(
        "first_name", initcap(regexp_replace(spark_trim(col("first_name")), " +", " "))
    ).withColumn(
        "last_name", initcap(regexp_replace(spark_trim(col("last_name")), " +", " "))
    )
    df = df.withColumn(
        "first_name", when(col("first_name") == "", None).otherwise(col("first_name"))
    ).withColumn(
        "last_name", when(col("last_name") == "", None).otherwise(col("last_name"))
    )
    df = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))

    # Etape 8 : validation email
    df = df.withColumn("email_clean", lower(spark_trim(regexp_replace(col("email"), " ", ""))))
    df = df.withColumn(
        "is_email_valid",
        col("email_clean").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    )

    # Etape 9 : telephone au format international
    df = df.withColumn("phone_clean", regexp_replace(col("phone"), " ", ""))
    df = df.withColumn(
        "phone_clean",
        when(col("phone_clean").rlike(r"^0033"), concat(lit("+33"), col("phone_clean").substr(5, 20)))
        .when(col("phone_clean").rlike(r"^\+33"), col("phone_clean"))
        .when(col("phone_clean").rlike(r"^0[0-9]{9}$"), concat(lit("+33"), col("phone_clean").substr(2, 20)))
        .otherwise(None)
    )

    # Etape 10 : villes / pays normalises
    df = df.withColumn(
        "country_clean",
        when(upper(spark_trim(col("country"))).isin("FR", "FRANCE", "FRENCH REPUBLIC"), "France")
        .otherwise(initcap(spark_trim(col("country"))))
    )
    df = df.withColumn(
        "city_clean",
        initcap(spark_trim(regexp_replace(col("city"), r"[\s-]?\d+$", "")))
    )

    # Etape 11 : date de naissance + age, rejet des ages impossibles
    df = df.withColumn(
        "birth_date_clean",
        coalesce(
            to_date(col("birth_date"), "yyyy-MM-dd"),
            to_date(col("birth_date"), "dd/MM/yyyy"),
            to_date(col("birth_date"), "yyyy/MM/dd"),
            to_date(col("birth_date"), "dd-MM-yyyy"),
        )
    )
    df = df.withColumn("age", floor(datediff(current_date(), col("birth_date_clean")) / 365.25))
    df = df.withColumn(
        "age", when((col("age") < 0) | (col("age") > 120), None).otherwise(col("age"))
    )

    # Etape 12 : deduplication par fenetre
    # Priorite : email valide > nombre de champs renseignes > created_at recent
    colonnes_a_verifier = ["email_clean", "phone_clean", "city_clean", "country_clean", "birth_date_clean"]
    df = df.withColumn(
        "nb_champs_renseignes",
        sum([when(col(c).isNotNull(), 1).otherwise(0) for c in colonnes_a_verifier])
    )
    fenetre = Window.partitionBy("customer_id_clean").orderBy(
        desc("is_email_valid"), desc("nb_champs_renseignes"), desc("created_at")
    )
    df = df.withColumn("rang_dedup", row_number().over(fenetre))
    customers_clean = df.filter(col("rang_dedup") == 1).drop("rang_dedup", "nb_champs_renseignes")

    return customers_clean


# ---------------------------------------------------------------------
# PARTIE 5 : NETTOYAGE DES COMMANDES
# ---------------------------------------------------------------------

def clean_orders(df_orders, taux_change_dict):
    """
    Etapes 13 a 17 : normalisation des commandes, montants, devises,
    conversion vers l'euro (jointure broadcast).
    Retourne (orders_clean, orders_rejects) : les rejets sont les
    commandes dont la date n'a pas pu etre convertie.
    """
    df = df_orders

    # Etape 13 : IDs au format ORD000001 + customer_id_clean + date
    df = df.withColumn(
        "order_id_clean",
        concat(lit("ORD"), lpad(regexp_replace(upper(regexp_replace(spark_trim(col("order_id")), "[-_ ]", "")), "[^0-9]", ""), 6, "0"))
    ).withColumn(
        "customer_id_clean",
        concat(lit("C"), lpad(regexp_replace(upper(regexp_replace(spark_trim(col("customer_id")), "[-_ ]", "")), "[^0-9]", ""), 6, "0"))
    )
    df = df.withColumn(
        "order_date_clean",
        coalesce(
            to_timestamp(col("order_date"), "yyyy-MM-dd"),
            to_timestamp(col("order_date"), "dd/MM/yyyy"),
            to_timestamp(col("order_date"), "yyyy/MM/dd"),
        )
    )
    df = df.withColumn("is_order_date_valid", col("order_date_clean").isNotNull())

    # Etape 14 : statuts normalises (7 valeurs cibles imposees par le sujet)
    df = df.withColumn(
        "status_clean",
        when(lower(spark_trim(col("status"))).isin("created"), "CREATED")
        .when(lower(spark_trim(col("status"))).isin("paid", "payée", "payee", "completed"), "PAID")
        .when(lower(spark_trim(col("status"))).isin("preparing", "en préparation"), "PREPARING")
        .when(lower(spark_trim(col("status"))).isin("shipped", "expédiée", "expediee"), "SHIPPED")
        .when(lower(spark_trim(col("status"))).isin("delivered", "livrée", "livree"), "DELIVERED")
        .when(lower(spark_trim(col("status"))).isin("cancelled", "canceled", "annulée", "annulee"), "CANCELLED")
        .when(lower(spark_trim(col("status"))).isin("returned", "retournée", "retournee"), "RETURNED")
        .otherwise("UNKNOWN")
    )

    # Mode de paiement normalise (necessaire pour customer_order_360)
    df = df.withColumn("payment_method_clean", upper(spark_trim(col("payment_method"))))

    # Etape 15 : montant, devise (EUR/USD/GBP/MAD), conversion euro via broadcast
    df = df.withColumn("total_amount_clean", col("total_amount").cast(DecimalType(12, 2)))
    df = df.withColumn("is_amount_valid", col("total_amount_clean") > 0)
    df = df.withColumn(
        "currency_clean",
        when(upper(spark_trim(col("currency"))).isin("EUR", "USD", "GBP", "MAD"), upper(spark_trim(col("currency"))))
        .otherwise("UNKNOWN")
    )
    df = df.withColumn("is_currency_valid", col("currency_clean") != "UNKNOWN")

    taux_rows = [(devise, taux) for devise, taux in taux_change_dict.items()]
    taux_change_df = df.sparkSession.createDataFrame(taux_rows, ["currency_clean", "taux"])
    df = df.join(broadcast(taux_change_df), "currency_clean", "left")
    df = df.withColumn(
        "total_amount_eur",
        (col("total_amount_clean") * col("taux")).cast(DecimalType(12, 2))
    )

    # Deduplication finale (priorite : montant valide > devise valide > date recente)
    fenetre = Window.partitionBy("order_id_clean").orderBy(
        desc("is_amount_valid"), desc("is_currency_valid"), desc("order_date_clean")
    )
    df = df.withColumn("rang_dedup", row_number().over(fenetre))
    orders_clean = df.filter(col("rang_dedup") == 1).drop("rang_dedup")

    orders_rejects = orders_clean.filter(col("is_order_date_valid") == False)

    return orders_clean, orders_rejects


# ---------------------------------------------------------------------
# PARTIE 6 : NETTOYAGE DES LIGNES DE COMMANDE
# ---------------------------------------------------------------------

def clean_order_items(df_order_items):
    """
    Etapes 16 et 17 : validation quantite/prix/remise, calcul
    gross/discount/net_amount, agregation par commande.
    Retourne (order_items_clean, order_items_summary, rejects).
    """
    df = df_order_items

    df = df.withColumn(
        "order_id_clean",
        concat(lit("ORD"), lpad(regexp_replace(upper(regexp_replace(spark_trim(col("order_id")), "[-_ ]", "")), "[^0-9]", ""), 6, "0"))
    ).withColumn(
        "product_id_clean", upper(spark_trim(col("product_id")))
    )

    df = df.withColumn(
        "discount_clean",
        when(col("discount").isNull(), 0.0)
        .when((col("discount") >= 0) & (col("discount") <= 1), col("discount"))
        .otherwise(None)
    ).withColumn(
        "is_quantity_valid", col("quantity") > 0
    ).withColumn(
        "is_price_valid", col("unit_price") > 0
    ).withColumn(
        "is_discount_valid", col("discount_clean").isNotNull()
    )

    # Etape 16 : gross / discount / net amount
    df = df.withColumn(
        "gross_amount",
        when(col("is_quantity_valid") & col("is_price_valid"), col("quantity") * col("unit_price")).otherwise(None)
    ).withColumn(
        "discount_amount",
        when(col("is_discount_valid"), col("gross_amount") * col("discount_clean")).otherwise(None)
    ).withColumn(
        "net_amount",
        when(col("gross_amount").isNotNull() & col("discount_amount").isNotNull(),
             col("gross_amount") - col("discount_amount")).otherwise(None)
    )

    order_items_rejects = df.filter(
        (col("is_quantity_valid") == False) | (col("is_price_valid") == False) | (col("is_discount_valid") == False)
    )

    order_items_summary = df.groupBy("order_id_clean").agg(
        countDistinct("product_id_clean").alias("number_of_products"),
        spark_sum(col("quantity")).alias("total_quantity"),
        spark_sum(col("gross_amount")).alias("total_gross_amount"),
        spark_sum(col("discount_amount")).alias("total_discount_amount"),
        spark_sum(col("net_amount")).alias("total_net_amount"),
    )

    return df, order_items_summary, order_items_rejects


# ---------------------------------------------------------------------
# PARTIE 7 : NETTOYAGE DES PRODUITS
# ---------------------------------------------------------------------

def clean_products(df_products):
    """
    Etape 18 : normalisation du catalogue, categorie unique
    "Technologie" regroupant High-Tech / Informatique / Electronique,
    puis deduplication par product_id_clean.
    """
    df = df_products

    df = df.withColumn(
        "product_id_clean", upper(spark_trim(col("product_id")))
    ).withColumn(
        "product_name_clean", initcap(regexp_replace(spark_trim(col("product_name")), " +", " "))
    ).withColumn(
        "brand_clean", upper(spark_trim(col("brand")))
    )

    df = df.withColumn(
        "category_clean",
        when(
            upper(regexp_replace(spark_trim(col("category")), "[-\\s]", "")).isin(
                "HIGHTECH", "INFORMATIQUE", "ELECTRONIQUE", "ÉLECTRONIQUE"
            ),
            "Technologie"
        ).otherwise(initcap(spark_trim(col("category"))))
    )

    df = df.withColumn("is_price_valid", col("current_price") > 0)
    df = df.withColumn("active_clean", when(col("active").isNull(), True).otherwise(col("active")))

    fenetre = Window.partitionBy("product_id_clean").orderBy(desc("is_price_valid"), desc("active_clean"))
    df = df.withColumn("rang_dedup", row_number().over(fenetre))
    products_clean = df.filter(col("rang_dedup") == 1).drop("rang_dedup")

    return products_clean


# ---------------------------------------------------------------------
# PARTIE 8 : NETTOYAGE DES AVIS MONGODB
# ---------------------------------------------------------------------

def clean_reviews(df_reviews, customers_clean, orders_clean, order_items_clean):
    """
    Etapes 19 a 22 : renommage des colonnes, validation des notes,
    deduplication (recence puis longueur du commentaire), et calcul
    de verified_purchase_computed / is_verification_consistent.
    Retourne (reviews_clean, reviews_rejects).
    """
    df = df_reviews.withColumn(
        "customer_id_clean",
        concat(lit("C"), lpad(regexp_replace(upper(regexp_replace(spark_trim(col("customerId")), "[-_ ]", "")), "[^0-9]", ""), 6, "0"))
    ).withColumn(
        "order_id_clean",
        concat(lit("ORD"), lpad(regexp_replace(upper(regexp_replace(spark_trim(col("orderId")), "[-_ ]", "")), "[^0-9]", ""), 6, "0"))
    ).withColumn(
        "product_id_clean", upper(spark_trim(col("productId")))
    ).withColumnRenamed(
        "reviewDate", "review_date"
    ).withColumnRenamed(
        "verifiedPurchase", "verified_purchase"
    )

    # Etape 20 : validation des notes + commentaires vides -> NULL
    df = df.withColumn("is_rating_valid", (col("rating") >= 1) & (col("rating") <= 5))
    df = df.withColumn(
        "comment_clean",
        when((spark_trim(col("comment")) == "") | col("comment").isNull(), None).otherwise(spark_trim(col("comment")))
    )
    reviews_rejects = df.filter(col("is_rating_valid") == False)

    # Etape 21 : deduplication (le plus recent, puis le commentaire le plus long)
    df = df.withColumn("comment_length", length(coalesce(col("comment_clean"), lit(""))))
    fenetre = Window.partitionBy("customer_id_clean", "order_id_clean", "product_id_clean").orderBy(
        desc("review_date"), desc("comment_length")
    )
    df = df.withColumn("rang_dedup", row_number().over(fenetre))
    reviews_clean = df.filter(col("rang_dedup") == 1).drop("rang_dedup")

    # Etape 22 : controle des achats verifies (client + commande + produit + coherence)
    verification = reviews_clean.alias("r") \
        .join(
            customers_clean.select("customer_id_clean").withColumnRenamed("customer_id_clean", "c_id"),
            col("r.customer_id_clean") == col("c_id"), "left"
        ) \
        .join(
            orders_clean.select("order_id_clean", "customer_id_clean")
                .withColumnRenamed("order_id_clean", "o_id")
                .withColumnRenamed("customer_id_clean", "o_customer_id"),
            col("r.order_id_clean") == col("o_id"), "left"
        ) \
        .join(
            order_items_clean.select("order_id_clean", "product_id_clean").distinct()
                .withColumnRenamed("order_id_clean", "oi_order_id")
                .withColumnRenamed("product_id_clean", "oi_product_id"),
            (col("r.order_id_clean") == col("oi_order_id")) & (col("r.product_id_clean") == col("oi_product_id")),
            "left"
        )

    verification = verification.withColumn(
        "verified_purchase_computed",
        col("c_id").isNotNull() & col("o_id").isNotNull() & col("oi_order_id").isNotNull() &
        (col("o_customer_id") == col("r.customer_id_clean"))
    )
    verification = verification.withColumn(
        "is_verification_consistent",
        col("verified_purchase_computed") == col("verified_purchase").cast("boolean")
    )
    reviews_clean = verification.select("r.*", "verified_purchase_computed", "is_verification_consistent")

    return reviews_clean, reviews_rejects


# ---------------------------------------------------------------------
# PARTIE 9 : NETTOYAGE DES EVENEMENTS DE LIVRAISON JSON
# ---------------------------------------------------------------------

def clean_delivery_events(df_delivery_events, orders_clean):
    """
    Etapes 23 a 27 : aplatissement du JSON, normalisation, suppression
    des vrais doublons (cle metier + priorite de statut), construction
    de delivery_summary, calcul du delai et de la performance.
    Retourne (delivery_events_clean, delivery_summary, rejects).
    """
    # Etape 23 : aplatissement des champs imbriques
    df = df_delivery_events.select(
        "event_id", "order_id", "event_type", "event_timestamp",
        col("location.city").alias("delivery_city"),
        col("location.country").alias("delivery_country"),
        col("carrier.id").alias("carrier_id"),
        col("carrier.name").alias("carrier_name"),
    )

    # Etape 24 : normalisation
    df = df.withColumn(
        "order_id_clean",
        concat(lit("ORD"), lpad(regexp_replace(upper(regexp_replace(spark_trim(col("order_id")), "[-_ ]", "")), "[^0-9]", ""), 6, "0"))
    ).withColumn(
        "event_type_clean",
        when(upper(spark_trim(col("event_type"))).isin(
            "ORDER_CREATED", "PREPARING", "SHIPPED", "IN_TRANSIT", "DELIVERED", "RETURNED"
        ), upper(spark_trim(col("event_type")))).otherwise("UNKNOWN")
    ).withColumn(
        "event_timestamp_clean", to_timestamp(col("event_timestamp"))
    ).withColumn(
        "delivery_city_clean", initcap(spark_trim(col("delivery_city")))
    ).withColumn(
        "delivery_country_clean", initcap(spark_trim(col("delivery_country")))
    ).withColumn(
        "carrier_name_clean", upper(spark_trim(col("carrier_name")))
    ).withColumn(
        "carrier_id_clean", upper(spark_trim(col("carrier_id")))
    )
    df = df.withColumn("is_timestamp_valid", col("event_timestamp_clean").isNotNull())

    rejects_statut_inconnu = df.filter(col("event_type_clean") == "UNKNOWN")

    # Etape 25 : suppression des vrais doublons (cle metier + priorite)
    priorite = (
        when(col("event_type_clean") == "DELIVERED", 1)
        .when(col("event_type_clean") == "RETURNED", 2)
        .when(col("event_type_clean") == "IN_TRANSIT", 3)
        .when(col("event_type_clean") == "SHIPPED", 4)
        .when(col("event_type_clean") == "PREPARING", 5)
        .when(col("event_type_clean") == "ORDER_CREATED", 6)
        .otherwise(7)
    )
    df = df.withColumn("priorite_event_type", priorite)
    fenetre_dedup = Window.partitionBy(
        "order_id_clean", "event_type_clean", "event_timestamp_clean", "carrier_id_clean"
    ).orderBy(col("priorite_event_type").asc())
    df = df.withColumn("rang_dedup", row_number().over(fenetre_dedup))
    delivery_events_clean = df.filter(col("rang_dedup") == 1).drop("rang_dedup", "priorite_event_type")

    # Etape 26 : delivery_summary (premier/dernier evenement par fenetre)
    fenetre_ordre = Window.partitionBy("order_id_clean").orderBy("event_timestamp_clean")
    fenetre_ordre_inv = Window.partitionBy("order_id_clean").orderBy(desc("event_timestamp_clean"))
    annote = delivery_events_clean.withColumn(
        "rang_premier", row_number().over(fenetre_ordre)
    ).withColumn(
        "rang_dernier", row_number().over(fenetre_ordre_inv)
    )

    premier = annote.filter(col("rang_premier") == 1).select(
        "order_id_clean",
        col("event_type_clean").alias("first_event_type"),
        col("event_timestamp_clean").alias("first_event_date"),
    )
    dernier = annote.filter(col("rang_dernier") == 1).select(
        "order_id_clean",
        col("event_type_clean").alias("last_status"),
        col("event_timestamp_clean").alias("last_event_date"),
        col("carrier_name_clean").alias("carrier"),
        col("delivery_city_clean").alias("delivery_city"),
    )
    date_expedition = delivery_events_clean.filter(col("event_type_clean") == "SHIPPED") \
        .groupBy("order_id_clean").agg(spark_min("event_timestamp_clean").alias("shipped_date"))
    date_livraison = delivery_events_clean.filter(col("event_type_clean") == "DELIVERED") \
        .groupBy("order_id_clean").agg(spark_min("event_timestamp_clean").alias("delivered_date"))
    nb_evenements = delivery_events_clean.groupBy("order_id_clean").agg(spark_count("*").alias("number_of_events"))

    delivery_summary = premier \
        .join(dernier, "order_id_clean", "left") \
        .join(date_expedition, "order_id_clean", "left") \
        .join(date_livraison, "order_id_clean", "left") \
        .join(nb_evenements, "order_id_clean", "left")

    # Etape 27 : delai et performance de livraison
    delivery_summary = delivery_summary.join(
        orders_clean.select("order_id_clean", col("order_date_clean").alias("order_date_ref")),
        "order_id_clean", "left"
    )
    delivery_summary = delivery_summary.withColumn(
        "delivery_delay_days",
        when(col("delivered_date").isNotNull() & col("order_date_ref").isNotNull(),
             datediff(to_date(col("delivered_date")), to_date(col("order_date_ref")))).otherwise(None)
    )
    delivery_summary = delivery_summary.withColumn(
        "delivery_performance",
        when(col("delivery_delay_days").isNull(), "Non livré")
        .when(col("delivery_delay_days") <= 2, "Très rapide")
        .when(col("delivery_delay_days") <= 5, "Normal")
        .when(col("delivery_delay_days") <= 10, "Lent")
        .otherwise("Très lent")
    )

    return delivery_events_clean, delivery_summary, rejects_statut_inconnu
