CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze."customers";

CREATE TABLE IF NOT EXISTS bronze."customers" (
    "customer_id" INTEGER PRIMARY KEY,
    "name" TEXT,
    "email" TEXT,
    "created_date" TEXT
);

DROP TABLE IF EXISTS bronze."orders";

CREATE TABLE IF NOT EXISTS bronze."orders" (
    "order_id" INTEGER PRIMARY KEY,
    "customer_id" INTEGER,
    "order_date" TEXT,
    "total_amount" FLOAT
);

DROP TABLE IF EXISTS bronze."order_items";

CREATE TABLE IF NOT EXISTS bronze."order_items" (
    "order_item_id" INTEGER PRIMARY KEY,
    "order_id" INTEGER,
    "product_id" INTEGER,
    "quantity" INTEGER,
    "unit_price" FLOAT
);

DROP TABLE IF EXISTS bronze."products";

CREATE TABLE IF NOT EXISTS bronze."products" (
    "product_id" INTEGER PRIMARY KEY,
    "product_name" TEXT,
    "category" TEXT,
    "price" FLOAT
);

DROP TABLE IF EXISTS bronze."fact_sales";

CREATE TABLE IF NOT EXISTS bronze."fact_sales" (
    "sale_id" INTEGER PRIMARY KEY,
    "order_id" INTEGER,
    "customer_id" INTEGER,
    "amount" FLOAT
);

DROP TABLE IF EXISTS bronze."dim_customer";

CREATE TABLE IF NOT EXISTS bronze."dim_customer" (
    "customer_id" INTEGER PRIMARY KEY,
    "name" TEXT
);

DROP TABLE IF EXISTS bronze."dim_product";

CREATE TABLE IF NOT EXISTS bronze."dim_product" (
    "product_id" INTEGER PRIMARY KEY,
    "product_name" TEXT
);

ALTER TABLE bronze."orders" ADD FOREIGN KEY ("customer_id") REFERENCES bronze."customers" ("customer_id");

ALTER TABLE bronze."order_items" ADD FOREIGN KEY ("order_id") REFERENCES bronze."orders" ("order_id");

ALTER TABLE bronze."order_items" ADD FOREIGN KEY ("product_id") REFERENCES bronze."customers" ("customer_id");

ALTER TABLE bronze."order_items" ADD FOREIGN KEY ("quantity") REFERENCES bronze."customers" ("customer_id");

ALTER TABLE bronze."fact_sales" ADD FOREIGN KEY ("order_id") REFERENCES bronze."orders" ("order_id");

ALTER TABLE bronze."fact_sales" ADD FOREIGN KEY ("customer_id") REFERENCES bronze."customers" ("customer_id");