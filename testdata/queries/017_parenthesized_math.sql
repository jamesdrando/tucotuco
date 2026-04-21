CREATE TABLE exprs (x INTEGER NOT NULL, y INTEGER NOT NULL);
INSERT INTO exprs VALUES (4, 1);
INSERT INTO exprs VALUES (6, 2);
SELECT (x + y) * 2 AS doubled_sum FROM exprs;
SELECT x + (y * 3) AS mixed_math FROM exprs;
