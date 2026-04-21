-- top-level comment with semicolon;
CREATE TABLE notes (id INTEGER NOT NULL, body VARCHAR(20) NOT NULL);
INSERT INTO notes VALUES (1, 'semi;colon'); /* block comment; kept outside statement */
SELECT id FROM notes;
