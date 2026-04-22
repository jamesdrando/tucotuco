CREATE TABLE readings (id INTEGER NOT NULL, amount INTEGER NOT NULL);
INSERT INTO readings VALUES (1, 10);
INSERT INTO readings VALUES (2, 20);
SELECT (SELECT 1) AS literal_one;
SELECT (SELECT amount FROM readings WHERE id = 2) AS amount_for_two;
SELECT (SELECT amount FROM readings WHERE id = 99) AS missing_amount;
SELECT id FROM readings WHERE amount = (SELECT amount FROM readings WHERE id = 1);
