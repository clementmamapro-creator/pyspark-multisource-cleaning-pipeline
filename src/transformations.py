# =====================================================================
# transformations.py
# Partie 10 : croisement des 5 sources nettoyees (jointures gauches,
#             sans multiplication de lignes).
# Partie 11 : construction de customer_order_360 + score de qualite.
# =====================================================================

from pyspark.sql.functions import (
    col, when, lit, coalesce, year, month, current_timestamp,
    countDistinct, collect_set, count as spark_count, avg as spark_avg,
    min as spark_min, max as spark_max, round as spark_round,
    abs as spark_abs,
)


def join_customers_orders(orders_clean, customers_clean):
    """Etape 28 : jointure gauche orders <- customers, avec customer_found."""
    result = orders_clean.join(
        customers_clean.select(
            "customer_id_clean", "full_name", "email_clean", "phone_clean",
            "city_clean", "country_clean"
        ),
        "customer_id_clean", "left"
    )
    result = result.withColumn("customer_found", col("full_name").isNotNull())
    return result


def join_order_items(orders_with_customer, order_items_summary):
    """Etape 29 : jointure avec l'agregation des lignes de commande + amount_difference."""
    result = orders_with_customer.join(order_items_summary, "order_id_clean", "left")
    result = result.withColumn(
        "amount_difference",
        coalesce(col("total_amount_eur"), lit(0)) - coalesce(col("total_net_amount"), lit(0))
    )
    result = result.withColumn("is_amount_consistent", spark_abs(col("amount_difference")) <= 0.01)
    return result


def join_products(orders_with_items, order_items_clean, products_clean):
    """
    Etape 30 : jointure avec les produits, en agregeant D'ABORD par
    commande (collect_set) pour eviter toute multiplication de lignes.
    """
    items_avec_produits = order_items_clean.join(
        products_clean.select("product_id_clean", "product_name_clean", "category_clean", "brand_clean"),
        "product_id_clean", "left"
    )
    produits_par_commande = items_avec_produits.groupBy("order_id_clean").agg(
        collect_set("product_name_clean").alias("product_names"),
        collect_set("category_clean").alias("product_categories"),
        collect_set("brand_clean").alias("product_brands"),
        countDistinct("category_clean").alias("number_of_categories"),
    )
    return orders_with_items.join(produits_par_commande, "order_id_clean", "left")


def join_reviews(orders_with_products, reviews_clean):
    """Etape 31 : jointure avec les avis (moyennes calculees sur notes valides uniquement)."""
    avis_par_commande = reviews_clean.groupBy("order_id_clean").agg(
        spark_count("*").alias("number_of_reviews"),
        spark_round(spark_avg(when(col("is_rating_valid"), col("rating"))), 2).alias("average_rating"),
        spark_min(when(col("is_rating_valid"), col("rating"))).alias("min_rating"),
        spark_max(when(col("is_rating_valid"), col("rating"))).alias("max_rating"),
        spark_count(when(col("verified_purchase_computed"), True)).alias("number_verified_purchases"),
        spark_count(col("comment_clean")).alias("number_of_comments"),
        spark_round(
            spark_count(when(col("is_rating_valid") & (col("rating") >= 4), True)) /
            spark_count(when(col("is_rating_valid"), True)) * 100, 2
        ).alias("positive_review_rate"),
    )
    result = orders_with_products.join(avis_par_commande, "order_id_clean", "left")
    result = result.withColumn(
        "customer_satisfaction",
        when(col("average_rating").isNull(), "Non évalué")
        .when(col("average_rating") >= 4, "Très satisfait")
        .when(col("average_rating") >= 3, "Satisfait")
        .when(col("average_rating") >= 2, "Peu satisfait")
        .otherwise("Insatisfait")
    )
    return result


def join_delivery(orders_with_reviews, delivery_summary):
    """Etape 32 : jointure avec le resume de livraison + delivery_found."""
    result = orders_with_reviews.join(
        delivery_summary.select(
            "order_id_clean", "last_status", "carrier", "shipped_date", "delivered_date",
            "delivery_delay_days", "delivery_performance", "number_of_events",
        ),
        "order_id_clean", "left"
    )
    result = result.withColumn("delivery_found", col("number_of_events").isNotNull())
    return result


def cross_all_sources(orders_clean, customers_clean, order_items_clean, order_items_summary,
                       products_clean, reviews_clean, delivery_summary):
    """Enchaine les 5 jointures des Etapes 28 a 32, dans l'ordre."""
    step1 = join_customers_orders(orders_clean, customers_clean)
    step2 = join_order_items(step1, order_items_summary)
    step3 = join_products(step2, order_items_clean, products_clean)
    step4 = join_reviews(step3, reviews_clean)
    step5 = join_delivery(step4, delivery_summary)
    return step5


def build_customer_order_360(orders_with_delivery):
    """Etape 33 : construit la vue finale avec les 35 colonnes attendues."""
    df = orders_with_delivery.select(
        col("order_id_clean").alias("order_id"),
        col("customer_id_clean").alias("customer_id"),
        "full_name",
        col("email_clean").alias("email"),
        col("phone_clean").alias("phone"),
        col("city_clean").alias("city"),
        col("country_clean").alias("country"),
        col("order_date_clean").alias("order_date"),
        year(col("order_date_clean")).alias("order_year"),
        month(col("order_date_clean")).alias("order_month"),
        col("status_clean").alias("order_status"),
        col("payment_method_clean").alias("payment_method"),
        col("currency_clean").alias("currency"),
        col("total_amount_eur").alias("declared_amount_eur"),
        col("total_net_amount").alias("calculated_amount_eur"),
        "amount_difference",
        "is_amount_consistent",
        "number_of_products",
        "total_quantity",
        "product_names",
        "product_categories",
        "product_brands",
        "number_of_reviews",
        "average_rating",
        "positive_review_rate",
        "customer_satisfaction",
        col("last_status").alias("last_delivery_status"),
        col("carrier").alias("carrier_name"),
        col("shipped_date").alias("shipping_date"),
        col("delivered_date").alias("delivery_date"),
        "delivery_delay_days",
        "delivery_performance",
        "customer_found",
        "delivery_found",
        current_timestamp().alias("processing_timestamp"),
    )
    return df


def add_quality_score(customer_order_360):
    """
    Etape 34 : score de qualite (100 - penalites), borne entre 0 et 100,
    puis quality_level (Excellent/Bon/Moyen/Faible).
    """
    df = customer_order_360.withColumn(
        "data_quality_score",
        lit(100)
        - when(col("customer_found") == False, 25).otherwise(0)
        - when((col("email").isNull()) | (~col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")), 10).otherwise(0)
        - when(col("phone").isNull(), 5).otherwise(0)
        - when(col("is_amount_consistent") == False, 20).otherwise(0)
        - when(col("delivery_found") == False, 10).otherwise(0)
        - when(col("order_status") == "UNKNOWN", 10).otherwise(0)
        - when(col("number_of_products").isNull() | (col("number_of_products") == 0), 20).otherwise(0)
        - when((col("number_of_reviews").isNotNull()) & (col("average_rating").isNull()), 5).otherwise(0)
    )
    df = df.withColumn(
        "data_quality_score",
        when(col("data_quality_score") < 0, 0)
        .when(col("data_quality_score") > 100, 100)
        .otherwise(col("data_quality_score"))
    )
    df = df.withColumn(
        "quality_level",
        when(col("data_quality_score") >= 90, "Excellent")
        .when(col("data_quality_score") >= 75, "Bon")
        .when(col("data_quality_score") >= 50, "Moyen")
        .otherwise("Faible")
    )
    return df
