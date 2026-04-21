CREATE TABLE orders (id INTEGER NOT NULL, total INTEGER NOT NULL);
INSERT INTO orders VALUES (1, 100);
INSERT INTO orders VALUES (2, 200);
UPDATE orders SET total = 250 WHERE id = 2;
SELECT id, total FROM orders WHERE id = 2;
