-- =====================================================================
-- analytical_queries.sql
-- Partie 15 - Les 10 questions analytiques, executees sur la vue
-- temporaire "customer_order_360_view" (creee par src/load.py).
-- Chaque requete est separee par un marqueur dedie, pour permettre
-- a load.py de les decouper et de les executer une a une.
-- =====================================================================

-- ### Q1 : Chiffre d'affaires mensuel par pays
SELECT country, order_year, order_month, ROUND(SUM(calculated_amount_eur), 2) AS chiffre_affaires
FROM customer_order_360_view
WHERE calculated_amount_eur IS NOT NULL
GROUP BY country, order_year, order_month
ORDER BY order_year, order_month, chiffre_affaires DESC;

-- ### Q2 : Cinq categories de produits generant le CA le plus eleve
SELECT category, ROUND(SUM(ca), 2) AS chiffre_affaires
FROM (
    SELECT EXPLODE(product_categories) AS category, calculated_amount_eur AS ca
    FROM customer_order_360_view
    WHERE calculated_amount_eur IS NOT NULL
)
GROUP BY category
ORDER BY chiffre_affaires DESC
LIMIT 5;

-- ### Q3 : Dix clients ayant depense le plus
SELECT customer_id, full_name, ROUND(SUM(calculated_amount_eur), 2) AS total_depense
FROM customer_order_360_view
WHERE calculated_amount_eur IS NOT NULL
GROUP BY customer_id, full_name
ORDER BY total_depense DESC
LIMIT 10;

-- ### Q4 : Note moyenne par categorie de produit
SELECT category, ROUND(AVG(average_rating), 2) AS note_moyenne
FROM (
    SELECT EXPLODE(product_categories) AS category, average_rating
    FROM customer_order_360_view
    WHERE average_rating IS NOT NULL
)
GROUP BY category
ORDER BY note_moyenne DESC;

-- ### Q5 : Transporteur avec le delai moyen de livraison le plus faible
SELECT carrier_name, ROUND(AVG(delivery_delay_days), 2) AS delai_moyen
FROM customer_order_360_view
WHERE delivery_delay_days IS NOT NULL
GROUP BY carrier_name
ORDER BY delai_moyen ASC;

-- ### Q6 : Pourcentage de commandes livrees en moins de 3 jours
SELECT ROUND(100.0 * SUM(CASE WHEN delivery_delay_days < 3 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_moins_3_jours
FROM customer_order_360_view
WHERE delivery_delay_days IS NOT NULL;

-- ### Q7 : Clients avec >= 3 commandes et note moyenne < 3
SELECT customer_id, full_name, COUNT(*) AS nb_commandes, ROUND(AVG(average_rating), 2) AS note_moyenne
FROM customer_order_360_view
GROUP BY customer_id, full_name
HAVING COUNT(*) >= 3 AND AVG(average_rating) < 3;

-- ### Q8 : Commandes avec montant declare different du montant recalcule
SELECT order_id, declared_amount_eur, calculated_amount_eur, amount_difference
FROM customer_order_360_view
WHERE is_amount_consistent = false
ORDER BY ABS(amount_difference) DESC;

-- ### Q9 : Pourcentage de commandes orphelines (client introuvable)
SELECT ROUND(100.0 * SUM(CASE WHEN customer_found = false THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_orphelines
FROM customer_order_360_view;

-- ### Q10 : Repartition des commandes selon leur niveau de qualite
SELECT quality_level, COUNT(*) AS nombre_commandes,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM customer_order_360_view
GROUP BY quality_level
ORDER BY nombre_commandes DESC;
