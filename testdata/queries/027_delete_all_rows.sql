CREATE TABLE tasks (id INTEGER NOT NULL, priority INTEGER NOT NULL);
INSERT INTO tasks VALUES (1, 1);
INSERT INTO tasks VALUES (2, 2);
DELETE FROM tasks WHERE priority >= 1;
SELECT id, priority FROM tasks;
