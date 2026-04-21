CREATE TABLE widgets (id INTEGER NOT NULL, code VARCHAR(12));
INSERT INTO widgets (id, code) VALUES (1);
INSERT INTO widgets (id, code) VALUES (1, 'x');
INSERT INTO widgets (id) SELECT 1, 2;
INSERT INTO widgets (id, code) SELECT 1;
