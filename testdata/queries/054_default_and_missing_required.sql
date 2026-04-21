CREATE TABLE widgets (id INTEGER NOT NULL, name VARCHAR(20) DEFAULT 'x');
INSERT INTO widgets (id) VALUES (1);
CREATE TABLE widgets_req (id INTEGER NOT NULL, code VARCHAR(12));
INSERT INTO widgets_req (code) VALUES ('x');
INSERT INTO widgets_req DEFAULT VALUES;
