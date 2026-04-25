CREATE SCHEMA sales;
CREATE TABLE sales.orders (id INTEGER NOT NULL, label VARCHAR(20));
INSERT INTO sales.orders VALUES (1, 'open');
SELECT sales.orders.id, sales.orders.label FROM sales.orders WHERE sales.orders.id = 1;
DROP SCHEMA sales;
DROP TABLE sales.orders;
DROP SCHEMA sales;
DROP SCHEMA sales;
