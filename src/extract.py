# =====================================================================
# extract.py
# Partie 1 : creation de la session Spark avec les connecteurs.
# Partie 2 : chargement brut des 3 sources (PostgreSQL, MongoDB, JSON).
#
# Aucun nettoyage ici : ce module se contente de lire les donnees
# telles quelles. Le nettoyage est entierement gere par cleaning.py.
# =====================================================================

import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


def load_config(config_path="config/application.conf"):
    """
    Lit le fichier de configuration (parametres non sensibles).
    Les mots de passe ne sont JAMAIS stockes ici : ils viennent des
    variables d'environnement (voir get_postgres_password ci-dessous).
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def get_postgres_password():
    """
    Recupere le mot de passe PostgreSQL depuis la variable
    d'environnement POSTGRES_PASSWORD. Leve une erreur explicite si
    elle n'est pas definie, plutot que d'echouer plus tard avec un
    message JDBC obscur.
    """
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise EnvironmentError(
            "La variable d'environnement POSTGRES_PASSWORD n'est pas definie. "
            "Exemple : export POSTGRES_PASSWORD=spark_password"
        )
    return password


def create_spark_session(config):
    """
    Etape 1 : cree la session Spark MultiSourceDataCleaning, avec les
    connecteurs JDBC (PostgreSQL) et MongoDB.
    Affiche les 4 informations demandees par le sujet : nom, mode,
    version, parallelisme par defaut.
    """
    app_name = config.get("spark", "app_name")
    master = config.get("spark", "master")

    mongo_uri = "mongodb://{}:{}/{}".format(
        config.get("mongodb", "host"),
        config.get("mongodb", "port"),
        config.get("mongodb", "database"),
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
        )
        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        # Algorithme de commit v2 : reduit les dossiers temporaires
        # imbriques, utile sur les environnements de developpement.
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    print("=== Partie 1 : session Spark ===")
    print("Nom de l'application :", spark.sparkContext.appName)
    print("Mode d'execution     :", spark.sparkContext.master)
    print("Version de Spark     :", spark.version)
    print("Parallelisme defaut  :", spark.sparkContext.defaultParallelism)

    return spark


def _jdbc_url(config):
    return "jdbc:postgresql://{}:{}/{}".format(
        config.get("postgres", "host"),
        config.get("postgres", "port"),
        config.get("postgres", "database"),
    )


def _jdbc_properties(config):
    return {
        "user": config.get("postgres", "user"),
        "password": get_postgres_password(),
        "driver": "org.postgresql.Driver",
    }


def extract_customers(spark, config):
    """Etape 2 : chargement JDBC de la table customers."""
    df = spark.read.jdbc(url=_jdbc_url(config), table="customers", properties=_jdbc_properties(config))
    _print_source_summary(df, "customers")
    return df


def extract_orders(spark, config):
    """Etape 2 : chargement JDBC de la table orders."""
    df = spark.read.jdbc(url=_jdbc_url(config), table="orders", properties=_jdbc_properties(config))
    _print_source_summary(df, "orders")
    return df


def extract_order_items(spark, config):
    """Etape 2 : chargement JDBC de la table order_items."""
    df = spark.read.jdbc(url=_jdbc_url(config), table="order_items", properties=_jdbc_properties(config))
    _print_source_summary(df, "order_items")
    return df


def extract_products(spark, config):
    """Etape 2 : chargement JDBC de la table products."""
    df = spark.read.jdbc(url=_jdbc_url(config), table="products", properties=_jdbc_properties(config))
    _print_source_summary(df, "products")
    return df


def extract_reviews(spark, config):
    """
    Etape 3 : chargement de la collection MongoDB "reviews".
    Ce dataset n'a pas de structure imbriquee a aplatir (contrairement
    a delivery_events), donc aucun traitement supplementaire ici.
    """
    collection = config.get("mongodb", "collection")
    df = (
        spark.read
        .format("mongodb")
        .option("database", config.get("mongodb", "database"))
        .option("collection", collection)
        .load()
    )
    _print_source_summary(df, "reviews (MongoDB)")
    return df


# Schema explicite du JSON des evenements de livraison (Etape 4).
# Impose par le sujet : ne pas se contenter de l'inference automatique.
DELIVERY_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
    ]), True),
    StructField("carrier", StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]), True),
])


def extract_delivery_events(spark, config):
    """
    Etape 4 : chargement de tous les fichiers JSON du dossier
    delivery_events/, avec un schema explicite (pas d'inference).
    """
    delivery_dir = config.get("paths", "delivery_events_dir")
    df = (
        spark.read
        .option("multiline", "true")
        .schema(DELIVERY_EVENTS_SCHEMA)
        .json(delivery_dir)
    )
    _print_source_summary(df, "delivery_events (JSON)")
    return df


def _print_source_summary(df, nom_source):
    """Affiche schema, 5 lignes, nombre de lignes et de partitions (Etape 2)."""
    print(f"\n=== Source : {nom_source} ===")
    df.printSchema()
    df.show(5, truncate=False)
    print("Nombre de lignes      :", df.count())
    print("Nombre de partitions  :", df.rdd.getNumPartitions())
