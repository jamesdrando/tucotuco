CREATE TABLE ledger (id INTEGER NOT NULL, balance INTEGER NOT NULL);
INSERT INTO ledger VALUES (1, 1000);
INSERT INTO ledger VALUES (2, 2000);
UPDATE ledger SET balance = 1500 WHERE id = 1;
SELECT id, balance FROM ledger;
