-- =====================================================================
-- create_tables.sql
-- Creation des 4 tables de la base relationnelle "ecommerce".
-- Types en VARCHAR/texte volontairement larges : les donnees sources
-- sont sales (formats heterogenes), le nettoyage reel se fait dans
-- Spark (src/cleaning.py), pas au niveau du schema PostgreSQL.
-- =====================================================================

-- Table des clients
CREATE TABLE customers (
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    phone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    birth_date VARCHAR(20),
    created_at TIMESTAMP
);

-- Table des commandes
CREATE TABLE orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date VARCHAR(20),
    status VARCHAR(50),
    payment_method VARCHAR(50),
    total_amount DECIMAL(12,2),
    currency VARCHAR(10)
);

-- Table des lignes de commande
CREATE TABLE order_items (
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(12,2),
    discount DECIMAL(5,2)
);

-- Table des produits
CREATE TABLE products (
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    brand VARCHAR(100),
    current_price DECIMAL(12,2),
    active BOOLEAN
);
