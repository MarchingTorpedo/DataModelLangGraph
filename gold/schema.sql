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