CREATE TABLE accounts (id INTEGER NOT NULL, balance INTEGER NOT NULL);
INSERT INTO accounts VALUES (1, 100);
INSERT INTO accounts VALUES (2, 250);
SELECT id AS account_id, balance AS current_balance FROM accounts;
SELECT balance - 10 AS after_fee FROM accounts;
