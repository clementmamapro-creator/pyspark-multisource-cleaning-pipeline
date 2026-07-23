# TP avancé PySpark — Nettoyage, croisement et consolidation de données multisources

**Master 2 — Big Data / Data Engineering**

## Contexte

Une entreprise e-commerce fictive répartit ses données dans trois systèmes hétérogènes, volontairement dégradés (identifiants incohérents, dates multi-formats, doublons, montants négatifs, statuts non normalisés) :

| Source | Contenu | Technologie |
|---|---|---|
| Base relationnelle | `customers`, `orders`, `order_items`, `products` | PostgreSQL (JDBC) |
| MongoDB | `reviews` | Connecteur Spark-MongoDB |
| Fichiers JSON | `delivery_events` | Lecture avec schéma explicite |

L'objectif est de nettoyer, croiser et consolider ces sources en une vue unique **`customer_order_360`**, enrichie d'indicateurs de qualité et de satisfaction client, puis d'en tirer des analyses via Spark SQL.

## Architecture du projet

```
tp_multisource/
├── src/
│   ├── main.py              # Orchestrateur du pipeline complet
│   ├── extract.py           # Partie 1-2 : session Spark + chargement des sources
│   ├── cleaning.py          # Parties 4-9 : nettoyage individuel de chaque source
│   ├── transformations.py   # Parties 10-11 : croisement + vue consolidée + score qualité
│   ├── quality.py           # Parties 3, 12, 13 : audit qualité, contrôles, rejets
│   └── load.py              # Parties 14-16 : écriture finale, requêtes SQL, optimisation
├── config/
│   └── application.conf     # Paramètres non sensibles (hôtes, ports, taux de change)
├── sql/
│   ├── create_tables.sql    # Création des 4 tables PostgreSQL
│   └── analytical_queries.sql  # Les 10 requêtes analytiques de la Partie 15
├── data/
│   ├── reference/           # Données de référence (taux de change, etc.)
│   └── delivery_events/     # Fichiers JSON des événements de livraison
├── output/
│   ├── customer_order_360/  # Vue finale, Parquet partitionné par année/mois
│   ├── data_quality_report/ # Rapport de qualité initial
│   └── rejects/             # Lignes rejetées, organisées par source
├── requirements.txt
└── README.md
```

## Prérequis

- Un cluster Spark accessible (local ou distant) avec PySpark 3.5.1
- PostgreSQL, contenant les tables créées via `sql/create_tables.sql`
- MongoDB, contenant la collection `reviews`
- La variable d'environnement `POSTGRES_PASSWORD` définie (le mot de passe n'est **jamais** écrit en dur dans le code ou dans `application.conf`)

## Lancement

```bash
export POSTGRES_PASSWORD=spark_password

spark-submit \
    --master local[*] \
    --packages org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
    src/main.py
```

Sur le cluster Docker du projet (`spark://spark-master:7077`, déjà configuré dans `config/application.conf`), lancer simplement depuis le conteneur `jupyter` ou tout client ayant accès au cluster :

```bash
spark-submit src/main.py
```

Pour exécuter en plus les démonstrations d'optimisation de la Partie 16 (`explain`, cache, `repartition`/`coalesce`) :

```bash
RUN_OPTIMISATION_DEMO=true spark-submit src/main.py
```

## Résultats obtenus (jeu de données de test, 40 commandes)

| Étape | Résultat |
|---|---|
| Audit qualité (Partie 3) | `phone` 16,7 % de nulles, `currency` 32,5 %, `discount` 21,3 % |
| Nettoyage clients (Partie 4) | 30 clients, aucun doublon détecté |
| Nettoyage commandes (Partie 5) | 40 commandes, 7 dates invalides rejetées, devise `MAD` reconnue |
| Nettoyage lignes de commande (Partie 6) | 73 lignes valides sur 80, 7 doublons supprimés |
| Nettoyage produits (Partie 7) | 15 produits, catégorie unique `Technologie` |
| Nettoyage avis (Partie 8) | 30 avis, 1 vrai doublon d'événement détecté après correction de la clé |
| Nettoyage livraisons (Partie 9) | 59 événements sur 60, `delivery_summary` avec délai/performance |
| Croisement (Partie 10) | 1 commande orpheline (`ORD000001`, client `C999` introuvable) |
| Vue finale (Partie 11) | `customer_order_360` : 40 lignes, score qualité 0-100 |
| Contrôles (Partie 12) | 1 anomalie réelle : 2 commandes annulées mais marquées livrées |
| Rejets (Partie 13) | 84 lignes rejetées au total, conservées avec motif et données d'origine |
| Écriture finale (Partie 14) | Parquet partitionné par `order_year`/`order_month`, rechargement validé |

## Points d'attention et limites connues

- **Devise par défaut** : une devise `NULL` est traitée en `EUR` par défaut (règle métier assumée, à ajuster selon le contexte réel).
- **Taux de change statiques** : `config/application.conf` contient des taux figés ; en production, ils devraient provenir d'une API de change mise à jour régulièrement.
- **`verified_purchase_computed`** : sur ce jeu de données synthétique, 100 % des avis sont calculés comme non vérifiés, car les combinaisons client/commande/produit de la collection `reviews` ne correspondent à aucune relation réelle dans `orders`/`order_items` — signal de qualité de données à traiter en amont dans un vrai contexte métier, pas un défaut du code.
- **Cache (Partie 16)** : sur ce volume réduit et un cluster à un seul worker, la mise en cache de `customer_order_360` s'est révélée plus lente que son absence — le cache n'est rentable qu'à partir d'un certain volume de données et d'un cluster à plusieurs workers.
- **Environnement Docker/Windows** : les écritures Parquet vers un volume monté depuis Windows (bind mount) peuvent échouer sur la création des dossiers temporaires (`_temporary`). Solution retenue : un volume Docker natif dédié aux sorties (`spark-output`), et les conteneurs Spark exécutés avec `user: root` pour éviter les blocages de permissions.

## Réponses aux questions de réflexion (Partie 17)

Voir le document séparé `Partie17_Questions_Reflexion.docx`, qui répond aux 15 questions du sujet en s'appuyant sur les observations concrètes de ce projet (normalisation des identifiants, `DecimalType`, jointures broadcast, `repartition`/`coalesce`, idempotence, gestion des rejets, etc.).

## Auteur

**Clément Ferry MAMA**
Étudiant en Data Engineering

*Projet réalisé dans le cadre de mon apprentissage — 2025/2026.*
# pyspark-multisource-cleaning-pipeline
