CREATE TABLE metrics (id INTEGER NOT NULL, status VARCHAR(10) NOT NULL, score INTEGER NOT NULL);
INSERT INTO metrics VALUES (1, 'new', 95);
INSERT INTO metrics VALUES (2, 'open', 70);
INSERT INTO metrics VALUES (3, 'closed', 50);
SELECT id, CASE WHEN score >= 80 THEN 'pass' WHEN score >= 60 THEN 'review' ELSE 'fail' END AS score_band FROM metrics;
SELECT id, CASE status WHEN 'new' THEN 'fresh' WHEN 'open' THEN 'active' ELSE 'closed' END AS status_label FROM metrics;
