CREATE TABLE stock (id INTEGER NOT NULL, qty INTEGER NOT NULL);
INSERT INTO stock VALUES (1, 5);
INSERT INTO stock VALUES (2, 15);
SELECT id, qty FROM stock WHERE qty > 10;
SELECT id + qty AS total FROM stock WHERE id = 1;
