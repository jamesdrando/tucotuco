CREATE TABLE orders (id INTEGER NOT NULL, customer_id INTEGER NOT NULL);
INSERT INTO orders VALUES (10, 1);
INSERT INTO orders VALUES (11, 3);
INSERT INTO orders VALUES (12, 3);
SELECT 1 IN (SELECT customer_id FROM orders);
SELECT 2 IN (SELECT customer_id FROM orders);
SELECT id FROM orders WHERE id = 11 AND customer_id IN (SELECT customer_id FROM orders WHERE customer_id = 3);
