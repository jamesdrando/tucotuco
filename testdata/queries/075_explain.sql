CREATE TABLE widgets (id INTEGER NOT NULL, name VARCHAR(20), active BOOLEAN);
INSERT INTO widgets VALUES (1, 'alice', TRUE);
INSERT INTO widgets VALUES (2, 'bob', FALSE);
CREATE VIEW active_widget_ids AS SELECT id FROM widgets WHERE active = TRUE;
EXPLAIN SELECT id, name FROM widgets WHERE id = 1;
EXPLAIN SELECT id FROM active_widget_ids WHERE id = 1;
EXPLAIN (SELECT id FROM widgets UNION ALL SELECT id FROM widgets);
EXPLAIN ANALYZE SELECT id FROM widgets;
