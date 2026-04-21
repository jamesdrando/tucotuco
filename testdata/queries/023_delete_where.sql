CREATE TABLE invoices (id INTEGER NOT NULL, amount INTEGER NOT NULL);
INSERT INTO invoices VALUES (1, 50);
INSERT INTO invoices VALUES (2, 75);
DELETE FROM invoices WHERE id = 1;
SELECT id, amount FROM invoices;
