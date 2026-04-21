CREATE TABLE metrics (id INTEGER NOT NULL, qty INTEGER NOT NULL);
INSERT INTO metrics VALUES (1, 10);
INSERT INTO metrics VALUES (2, 20);
SELECT id, qty FROM metrics;
SELECT qty + 1 AS next_qty FROM metrics;
