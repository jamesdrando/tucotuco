CREATE TABLE widgets (id INTEGER NOT NULL, code VARCHAR(12));
INSERT INTO widgets (code) VALUES ('x');
INSERT INTO widgets DEFAULT VALUES;
INSERT INTO widgets (id, code) VALUES (1, 'x');
UPDATE widgets SET id = NULL;
