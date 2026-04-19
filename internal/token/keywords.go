package token

import "strings"

// KeywordClass describes whether a SQL keyword is reserved.
type KeywordClass uint8

const (
	// KeywordNonReserved marks a keyword that can still act as an identifier.
	KeywordNonReserved KeywordClass = iota + 1
	// KeywordReserved marks a keyword that is never allowed as an identifier.
	KeywordReserved
)

// String returns the stable string form of the keyword class.
func (c KeywordClass) String() string {
	switch c {
	case KeywordNonReserved:
		return "non-reserved"
	case KeywordReserved:
		return "reserved"
	default:
		return "unknown"
	}
}

// Keyword carries the canonical SQL spelling plus reserved-word metadata.
type Keyword struct {
	Word  string
	Class KeywordClass
}

// IsReserved reports whether the keyword is reserved in the combined keyword table.
func (k Keyword) IsReserved() bool {
	return k.Class == KeywordReserved
}

// sql92KeywordSpecs is the single source of truth for the Phase 1 keyword set.
//
// Classifications are derived from PostgreSQL's SQL keyword appendix, which
// includes SQL-92 reserved/non-reserved status, and cross-checked against
// MonetDB's SQL standard keyword table.
var sql92KeywordSpecs = append(
	makeKeywordSpecs(sql92ReservedWords, KeywordReserved),
	makeKeywordSpecs(sql92NonReservedWords, KeywordNonReserved)...,
)

// sql99KeywordSpecs contains the SQL:1999 additions that later parser work
// will need to recognize without changing the phase 1 lexer behavior.
var sql99KeywordSpecs = makeKeywordSpecs(sql99NonReservedWords, KeywordNonReserved)

var (
	sql92KeywordTable = buildKeywordTable(sql92KeywordSpecs)
	keywordTable      = buildKeywordTable(combinedKeywordSpecs())
)

// SQL92Keywords returns a copy of the Phase 1 SQL-92 keyword set.
func SQL92Keywords() []Keyword {
	keywords := make([]Keyword, len(sql92KeywordSpecs))
	copy(keywords, sql92KeywordSpecs)

	return keywords
}

// SQL99Keywords returns a copy of the SQL:1999 keyword additions.
func SQL99Keywords() []Keyword {
	keywords := make([]Keyword, len(sql99KeywordSpecs))
	copy(keywords, sql99KeywordSpecs)

	return keywords
}

// LookupSQL92Keyword performs a case-insensitive SQL-92 keyword lookup.
func LookupSQL92Keyword(word string) (Keyword, bool) {
	return lookupKeyword(sql92KeywordTable, word)
}

// LookupKeyword performs a case-insensitive SQL keyword lookup.
func LookupKeyword(word string) (Keyword, bool) {
	return lookupKeyword(keywordTable, word)
}

func lookupKeyword(table map[string]Keyword, word string) (Keyword, bool) {
	if word == "" {
		return Keyword{}, false
	}

	keyword, ok := table[foldKeyword(word)]

	return keyword, ok
}

// IsKeyword reports whether the input is a SQL keyword.
func IsKeyword(word string) bool {
	_, ok := LookupKeyword(word)

	return ok
}

// IsReservedKeyword reports whether the input is a reserved SQL keyword.
func IsReservedKeyword(word string) bool {
	keyword, ok := LookupKeyword(word)
	if !ok {
		return false
	}

	return keyword.IsReserved()
}

func makeKeywordSpecs(words string, class KeywordClass) []Keyword {
	fields := strings.Fields(words)
	specs := make([]Keyword, 0, len(fields))
	for _, field := range fields {
		specs = append(specs, Keyword{
			Word:  field,
			Class: class,
		})
	}

	return specs
}

func buildKeywordTable(specs []Keyword) map[string]Keyword {
	table := make(map[string]Keyword, len(specs))
	for _, spec := range specs {
		table[spec.Word] = spec
	}

	return table
}

func combinedKeywordSpecs() []Keyword {
	specs := make([]Keyword, 0, len(sql92KeywordSpecs)+len(sql99KeywordSpecs))
	specs = append(specs, sql92KeywordSpecs...)
	specs = append(specs, sql99KeywordSpecs...)

	return specs
}

func foldKeyword(word string) string {
	var builder strings.Builder
	changed := false

	for index := 0; index < len(word); index++ {
		ch := word[index]
		if ch >= 'a' && ch <= 'z' {
			if !changed {
				builder.Grow(len(word))
				builder.WriteString(word[:index])
				changed = true
			}

			builder.WriteByte(ch - 'a' + 'A')
			continue
		}

		if changed {
			builder.WriteByte(ch)
		}
	}

	if !changed {
		return word
	}

	return builder.String()
}

const sql92ReservedWords = `
ABSOLUTE ACTION ADD ALL ALLOCATE ALTER AND ANY ARE AS ASC ASSERTION AT AUTHORIZATION
AVG BEGIN BETWEEN BIT BIT_LENGTH BOTH BY CASCADE CASCADED CASE CAST CATALOG CHAR
CHARACTER CHARACTER_LENGTH CHAR_LENGTH CHECK CLOSE COALESCE COLLATE COLLATION COLUMN
COMMIT CONNECT CONNECTION CONSTRAINT CONSTRAINTS CONTINUE CONVERT CORRESPONDING COUNT
CREATE CROSS CURRENT CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURSOR
DATE DAY DEALLOCATE DEC DECIMAL DECLARE DEFAULT DEFERRABLE DEFERRED DELETE DESC
DESCRIBE DESCRIPTOR DIAGNOSTICS DISCONNECT DISTINCT DOMAIN DOUBLE DROP ELSE END
END-EXEC ESCAPE EXCEPT EXCEPTION EXEC EXECUTE EXISTS EXTERNAL EXTRACT FALSE FETCH
FIRST FLOAT FOR FOREIGN FOUND FROM FULL GET GLOBAL GO GOTO GRANT GROUP HAVING HOUR
IDENTITY IMMEDIATE IN INDICATOR INITIALLY INNER INPUT INSENSITIVE INSERT INT INTEGER
INTERSECT INTERVAL INTO IS ISOLATION JOIN KEY LANGUAGE LAST LEADING LEFT LEVEL LIKE
LOCAL LOWER MATCH MAX MIN MINUTE MODULE MONTH NAMES NATIONAL NATURAL NCHAR NEXT NO
NOT NULL NULLIF NUMERIC OCTET_LENGTH OF ON ONLY OPEN OPTION OR ORDER OUTER OUTPUT
OVERLAPS PAD PARTIAL POSITION PRECISION PREPARE PRESERVE PRIMARY PRIOR PRIVILEGES
PROCEDURE PUBLIC READ REAL REFERENCES RELATIVE RESTRICT REVOKE RIGHT ROLLBACK ROWS
SCHEMA SCROLL SECOND SECTION SELECT SESSION SESSION_USER SET SIZE SMALLINT SOME SPACE
SQL SQLCODE SQLERROR SQLSTATE SUBSTRING SUM SYSTEM_USER TABLE TEMPORARY THEN TIME
TIMESTAMP TIMEZONE_HOUR TIMEZONE_MINUTE TO TRAILING TRANSACTION TRANSLATE TRANSLATION
TRIM TRUE UNION UNIQUE UNKNOWN UPDATE UPPER USAGE USER USING VALUE VALUES VARCHAR
VARYING VIEW WHEN WHENEVER WHERE WITH WORK WRITE YEAR ZONE
`

const sql92NonReservedWords = `
ADA C CATALOG_NAME CHARACTER_SET_CATALOG CHARACTER_SET_NAME CHARACTER_SET_SCHEMA
CLASS_ORIGIN COBOL COLLATION_CATALOG COLLATION_NAME COLLATION_SCHEMA COLUMN_NAME
COMMAND_FUNCTION COMMITTED CONDITION_NUMBER CONNECTION_NAME CONSTRAINT_CATALOG
CONSTRAINT_NAME CONSTRAINT_SCHEMA CURSOR_NAME DATA DATETIME_INTERVAL_CODE
DATETIME_INTERVAL_PRECISION DYNAMIC_FUNCTION FORTRAN LENGTH MESSAGE_LENGTH
MESSAGE_OCTET_LENGTH MORE MUMPS NAME NULLABLE NUMBER PASCAL PLI REPEATABLE
RETURNED_LENGTH RETURNED_OCTET_LENGTH RETURNED_SQLSTATE ROW_COUNT SCALE SCHEMA_NAME
SERIALIZABLE SERVER_NAME SUBCLASS_ORIGIN TABLE_NAME TYPE UNCOMMITTED UNNAMED
`

const sql99NonReservedWords = `
AFTER BEFORE BREADTH CUBE CYCLE DEPTH EACH ELSEIF EXIT GROUPING HANDLER IF INSTEAD
ITERATE LEAVE LOOP NEW OLD RECURSIVE REFERENCING REPEAT RESIGNAL RETURN ROLLUP ROW
SEARCH SETS SIGNAL STATEMENT UNTIL WHILE UNDO
`
