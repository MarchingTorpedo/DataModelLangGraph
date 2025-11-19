CREATE TABLE "customers" (
    "customer_id" INTEGER PRIMARY KEY,
    "name" TEXT,
    "email" TEXT,
    "created_date" TEXT
);

CREATE TABLE "orders" (
    "order_id" INTEGER PRIMARY KEY,
    "customer_id" INTEGER,
    "order_date" TEXT,
    "total_amount" FLOAT
);

CREATE TABLE "order_items" (
    "order_item_id" INTEGER PRIMARY KEY,
    "order_id" INTEGER,
    "product_id" INTEGER,
    "quantity" INTEGER,
    "unit_price" FLOAT
);

CREATE TABLE "products" (
    "product_id" INTEGER PRIMARY KEY,
    "product_name" TEXT,
    "category" TEXT,
    "price" FLOAT
);

CREATE TABLE "fact_sales" (
    "sale_id" INTEGER PRIMARY KEY,
    "order_id" INTEGER,
    "customer_id" INTEGER,
    "amount" FLOAT
);

CREATE TABLE "dim_customer" (
    "customer_id" INTEGER PRIMARY KEY,
    "name" TEXT
);

CREATE TABLE "dim_product" (
    "product_id" INTEGER PRIMARY KEY,
    "product_name" TEXT
);

ALTER TABLE "orders" ADD FOREIGN KEY ("customer_id") REFERENCES "customers" ("customer_id");

ALTER TABLE "order_items" ADD FOREIGN KEY ("order_id") REFERENCES "orders" ("order_id");

ALTER TABLE "order_items" ADD FOREIGN KEY ("product_id") REFERENCES "customers" ("customer_id");

ALTER TABLE "order_items" ADD FOREIGN KEY ("quantity") REFERENCES "customers" ("customer_id");

ALTER TABLE "fact_sales" ADD FOREIGN KEY ("order_id") REFERENCES "orders" ("order_id");

ALTER TABLE "fact_sales" ADD FOREIGN KEY ("customer_id") REFERENCES "customers" ("customer_id");