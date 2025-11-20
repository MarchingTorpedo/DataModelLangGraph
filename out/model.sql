CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver."customers" (
    "customer_id" INTEGER PRIMARY KEY,
    "name" TEXT,
    "email" TEXT,
    "created_date" TEXT
);

CREATE TABLE IF NOT EXISTS silver."orders" (
    "order_id" INTEGER PRIMARY KEY,
    "customer_id" INTEGER,
    "order_date" TEXT,
    "total_amount" FLOAT
);

CREATE TABLE IF NOT EXISTS silver."order_items" (
    "order_item_id" INTEGER PRIMARY KEY,
    "order_id" INTEGER,
    "product_id" INTEGER,
    "quantity" INTEGER,
    "unit_price" FLOAT
);

CREATE TABLE IF NOT EXISTS silver."products" (
    "product_id" INTEGER PRIMARY KEY,
    "product_name" TEXT,
    "category" TEXT,
    "price" FLOAT
);

CREATE TABLE IF NOT EXISTS silver."fact_sales" (
    "sale_id" INTEGER PRIMARY KEY,
    "order_id" INTEGER,
    "customer_id" INTEGER,
    "amount" FLOAT
);

CREATE TABLE IF NOT EXISTS silver."dim_customer" (
    "customer_id" INTEGER PRIMARY KEY,
    "name" TEXT
);

CREATE TABLE IF NOT EXISTS silver."dim_product" (
    "product_id" INTEGER PRIMARY KEY,
    "product_name" TEXT
);

ALTER TABLE silver."orders" ADD FOREIGN KEY ("customer_id") REFERENCES silver."customers" ("customer_id");

ALTER TABLE silver."order_items" ADD FOREIGN KEY ("order_id") REFERENCES silver."orders" ("order_id");

ALTER TABLE silver."order_items" ADD FOREIGN KEY ("product_id") REFERENCES silver."customers" ("customer_id");

ALTER TABLE silver."order_items" ADD FOREIGN KEY ("quantity") REFERENCES silver."customers" ("customer_id");

ALTER TABLE silver."fact_sales" ADD FOREIGN KEY ("order_id") REFERENCES silver."orders" ("order_id");

ALTER TABLE silver."fact_sales" ADD FOREIGN KEY ("customer_id") REFERENCES silver."customers" ("customer_id");