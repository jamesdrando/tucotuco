CREATE TABLE mixed (id INTEGER NOT NULL, amount INTEGER NOT NULL);
INSERT INTO mixed VALUES (1, 8);
INSERT INTO mixed VALUES (2, 13);
SELECT id, amount FROM mixed;
SELECT amount - id AS delta FROM mixed;
