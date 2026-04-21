CREATE TABLE metrics (id INTEGER NOT NULL, reading INTEGER);
INSERT INTO metrics VALUES (1, 10);
INSERT INTO metrics VALUES (2, NULL);
SELECT id FROM metrics WHERE reading IS NULL;
SELECT id, reading FROM metrics WHERE reading IS NOT NULL;
