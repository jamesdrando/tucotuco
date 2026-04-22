CREATE TABLE readings (group_id INTEGER NOT NULL, amount INTEGER NOT NULL, ok BOOLEAN NOT NULL);
INSERT INTO readings VALUES (1, 10, TRUE);
INSERT INTO readings VALUES (1, 20, TRUE);
INSERT INTO readings VALUES (2, 30, FALSE);
SELECT
  COUNT(*) AS row_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS average_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount,
  EVERY(ok) AS all_ok
FROM readings;
SELECT
  group_id,
  COUNT(*) AS row_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS average_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount,
  EVERY(ok) AS all_ok
FROM readings
GROUP BY group_id;
