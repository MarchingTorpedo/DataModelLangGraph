DROP TABLE IF EXISTS "customers";

CREATE TABLE IF NOT EXISTS "customers" (
    "customer_id" INTEGER PRIMARY KEY,
    "name" TEXT,
    "email" TEXT,
    "created_date" TEXT
);

DROP TABLE IF EXISTS "dim_customer";

CREATE TABLE IF NOT EXISTS "dim_customer" (
    "customer_id" INTEGER PRIMARY KEY,
    "name" TEXT
);

DROP TABLE IF EXISTS "dim_product";

CREATE TABLE IF NOT EXISTS "dim_product" (
    "product_id" INTEGER PRIMARY KEY,
    "product_name" TEXT
);

DROP TABLE IF EXISTS "fact_sales";

CREATE TABLE IF NOT EXISTS "fact_sales" (
    "sale_id" INTEGER PRIMARY KEY,
    "order_id" INTEGER,
    "customer_id" INTEGER,
    "amount" FLOAT
);

DROP TABLE IF EXISTS "orders";

CREATE TABLE IF NOT EXISTS "orders" (
    "order_id" INTEGER PRIMARY KEY,
    "customer_id" INTEGER,
    "order_date" TEXT,
    "total_amount" FLOAT
);

DROP TABLE IF EXISTS "order_items";

CREATE TABLE IF NOT EXISTS "order_items" (
    "order_item_id" INTEGER PRIMARY KEY,
    "order_id" INTEGER,
    "product_id" INTEGER,
    "quantity" INTEGER,
    "unit_price" FLOAT
);

DROP TABLE IF EXISTS "products";

CREATE TABLE IF NOT EXISTS "products" (
    "product_id" INTEGER PRIMARY KEY,
    "product_name" TEXT,
    "category" TEXT,
    "price" FLOAT
);

ALTER TABLE "orders" ADD FOREIGN KEY ("customer_id") REFERENCES "customers" ("customer_id");

ALTER TABLE "order_items" ADD FOREIGN KEY ("order_id") REFERENCES "orders" ("order_id");

ALTER TABLE "order_items" ADD FOREIGN KEY ("product_id") REFERENCES "customers" ("customer_id");

ALTER TABLE "order_items" ADD FOREIGN KEY ("quantity") REFERENCES "customers" ("customer_id");

ALTER TABLE "fact_sales" ADD FOREIGN KEY ("order_id") REFERENCES "orders" ("order_id");

ALTER TABLE "fact_sales" ADD FOREIGN KEY ("customer_id") REFERENCES "customers" ("customer_id");