package parser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/lexer"
	"github.com/jamesdrando/tucotuco/internal/token"
)

func TestParserPeekConsumeAndEOFDraindown(t *testing.T) {
	t.Parallel()

	first := testToken(token.KindIdentifier, "alpha", token.Pos{Line: 1, Column: 1, Offset: 0})
	semicolon := testToken(token.KindPunctuation, ";", token.Pos{Line: 1, Column: 6, Offset: 5})
	eof := token.Token{
		Kind: token.KindEOF,
		Span: token.Span{
			Start: token.Pos{Line: 1, Column: 7, Offset: 6},
			Stop:  token.Pos{Line: 1, Column: 7, Offset: 6},
		},
	}

	p := New([]token.Token{first, semicolon})

	if got := p.Peek(); got != first {
		t.Fatalf("Peek() = %#v, want %#v", got, first)
	}

	if got := p.Peek(); got != first {
		t.Fatalf("Peek() second call = %#v, want %#v", got, first)
	}

	if got := p.Consume(); got != first {
		t.Fatalf("Consume() = %#v, want %#v", got, first)
	}

	if got := p.Peek(); got != semicolon {
		t.Fatalf("Peek() after Consume() = %#v, want %#v", got, semicolon)
	}

	if got := p.Consume(); got != semicolon {
		t.Fatalf("Consume() second token = %#v, want %#v", got, semicolon)
	}

	if got := p.Peek(); got != eof {
		t.Fatalf("Peek() at EOF = %#v, want %#v", got, eof)
	}

	if got := p.Consume(); got != eof {
		t.Fatalf("Consume() at EOF = %#v, want %#v", got, eof)
	}

	if got := p.Peek(); got != eof {
		t.Fatalf("Peek() after EOF drain = %#v, want %#v", got, eof)
	}

	if got := p.Consume(); got != eof {
		t.Fatalf("Consume() after EOF drain = %#v, want %#v", got, eof)
	}
}

func TestParserStopsAtEmbeddedEOFAndNormalizesSentinel(t *testing.T) {
	t.Parallel()

	first := testToken(token.KindIdentifier, "alpha", token.Pos{Line: 1, Column: 1, Offset: 0})
	embeddedEOF := token.Token{
		Kind: token.KindEOF,
		Span: token.Span{
			Start: token.Pos{Line: 1, Column: 6, Offset: 5},
			Stop:  token.Pos{Line: 1, Column: 6, Offset: 5},
		},
	}
	trailing := testToken(token.KindIdentifier, "omega", token.Pos{Line: 1, Column: 7, Offset: 6})

	p := New([]token.Token{first, embeddedEOF, trailing})

	if got, want := len(p.tokens), 2; got != want {
		t.Fatalf("len(p.tokens) = %d, want %d", got, want)
	}

	if got := p.tokens[len(p.tokens)-1]; got.Kind != token.KindEOF {
		t.Fatalf("last token = %#v, want EOF sentinel", got)
	}

	if got := p.Peek(); got != first {
		t.Fatalf("Peek() = %#v, want %#v", got, first)
	}

	if got := p.Consume(); got != first {
		t.Fatalf("Consume() = %#v, want %#v", got, first)
	}

	if got := p.Peek(); got != embeddedEOF {
		t.Fatalf("Peek() after consuming first token = %#v, want embedded EOF %#v", got, embeddedEOF)
	}

	if got := p.Consume(); got != embeddedEOF {
		t.Fatalf("Consume() embedded EOF = %#v, want %#v", got, embeddedEOF)
	}

	if got := p.Peek(); got.Kind != token.KindEOF {
		t.Fatalf("Peek() after embedded EOF = %#v, want EOF sentinel", got)
	}

	if got := p.Consume(); got.Kind != token.KindEOF {
		t.Fatalf("Consume() after embedded EOF = %#v, want EOF sentinel", got)
	}
}

func TestParserRecoverStatementSkipsToSemicolonAfterLexerError(t *testing.T) {
	t.Parallel()

	p := New(lexer.NewString("@; SELECT 2;").All())

	if got := p.Peek(); got.Kind != token.KindError {
		t.Fatalf("Peek() = %v, want lexer error token", got)
	}

	p.recoverStatement()

	errs := p.Errors()
	if len(errs) != 1 {
		t.Fatalf("Errors() length = %d, want 1", len(errs))
	}

	if got, want := errs[0].Error(), "ERROR [SQLSTATE 42601] at 1:1 (offset 0): unexpected character"; got != want {
		t.Fatalf("Errors()[0].Error() = %q, want %q", got, want)
	}

	if got := p.Peek(); got.Kind != token.KindKeyword || !got.IsKeyword("SELECT") {
		t.Fatalf("Peek() after recovery = %v, want SELECT keyword", got)
	}

	if got := p.Consume(); got.Kind != token.KindKeyword || !got.IsKeyword("SELECT") {
		t.Fatalf("Consume() after recovery = %v, want SELECT keyword", got)
	}

	if got := p.Consume(); got.Kind != token.KindInteger || got.Lexeme != "2" {
		t.Fatalf("Consume() after recovery = %v, want integer literal 2", got)
	}

	if got := p.Consume(); got.Kind != token.KindPunctuation || got.Lexeme != ";" {
		t.Fatalf("Consume() after recovery = %v, want semicolon", got)
	}

	if got := p.Peek(); got.Kind != token.KindEOF {
		t.Fatalf("Peek() after draining recovered statement = %v, want EOF", got)
	}
}

func TestParseExpr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "integer", input: "42", want: "int(42)"},
		{name: "decimal", input: "3.14", want: "float(3.14)"},
		{name: "scientific", input: "6.02e23", want: "float(6.02e23)"},
		{name: "hex", input: "0x2A", want: "float(0x2A)"},
		{name: "string", input: "'hello'", want: `string("hello")`},
		{name: "bool true", input: "TRUE", want: "bool(true)"},
		{name: "bool false", input: "FALSE", want: "bool(false)"},
		{name: "null", input: "NULL", want: "null"},
		{name: "param marker", input: "?", want: "param(?)"},
		{name: "identifier lowercased", input: "CustomerID", want: "id(customerid)"},
		{name: "quoted identifier preserved", input: `"CustomerID"`, want: `qid("CustomerID")`},
		{name: "qualified name", input: "sales.orders", want: "name(sales.orders)"},
		{name: "qualified star", input: "orders.*", want: "star(name(orders).*)"},
		{name: "unary and multiplicative", input: "-a * b", want: "((- id(a)) * id(b))"},
		{name: "arithmetic precedence", input: "1 + 2 * 3", want: "(int(1) + (int(2) * int(3)))"},
		{name: "concatenation", input: "customer_id || '-' || order_id", want: "((id(customer_id) || string(\"-\")) || id(order_id))"},
		{name: "comparison after arithmetic", input: "a + b = c + d", want: "((id(a) + id(b)) = (id(c) + id(d)))"},
		{name: "logical precedence", input: "NOT a = b AND c OR d", want: "(((NOT (id(a) = id(b))) AND id(c)) OR id(d))"},
		{name: "between", input: "a BETWEEN 1 AND 2 OR b", want: "(between(id(a), int(1), int(2)) OR id(b))"},
		{name: "not between", input: "a NOT BETWEEN 1 AND 2", want: "not between(id(a), int(1), int(2))"},
		{name: "in list", input: "a IN (1, 2, 3)", want: "in(id(a), [int(1), int(2), int(3)])"},
		{name: "not in list", input: "a NOT IN (1, 2)", want: "not in(id(a), [int(1), int(2)])"},
		{name: "like escape", input: "a LIKE 'x%' ESCAPE '!'", want: `like(id(a), string("x%"), string("!"))`},
		{name: "is not null", input: "a IS NOT NULL", want: "is(id(a) NOT NULL)"},
		{name: "is distinct from", input: "a IS DISTINCT FROM b", want: "is(id(a) DISTINCT FROM id(b))"},
		{name: "simple function call", input: "coalesce(a, b, 0)", want: "call(name(coalesce), [id(a), id(b), int(0)])"},
		{name: "keyword function call", input: "COUNT(*)", want: "call(name(count), [star(*)])"},
		{name: "explicit all function call", input: "COUNT(ALL amount)", want: "call(name(count), ALL [id(amount)])"},
		{name: "qualified distinct function call", input: "analytics.COUNT(DISTINCT amount)", want: "call(name(analytics.count), DISTINCT [id(amount)])"},
		{name: "cast", input: "CAST(total AS INTEGER)", want: "cast(id(total) AS name(integer))"},
		{name: "cast with type args", input: "CAST(total AS CHARACTER VARYING(12))", want: "cast(id(total) AS name(character.varying)([int(12)]))"},
		{name: "searched case", input: "CASE WHEN a = 1 THEN 'x' ELSE 'y' END", want: `case([when((id(a) = int(1)) => string("x"))], else=string("y"))`},
		{name: "simple case", input: "CASE status WHEN 'a' THEN 1 WHEN 'b' THEN 2 ELSE 0 END", want: `case(id(status), [when(string("a") => int(1)), when(string("b") => int(2))], else=int(0))`},
		{name: "comments ignored", input: "a /* mid */ + 1", want: "(id(a) + int(1))"},
		{name: "non reserved keyword as identifier", input: "type", want: "id(type)"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, errs := parseExprString(tc.input)
			if len(errs) != 0 {
				t.Fatalf("ParseExpr(%q) errors = %v, want none", tc.input, errorsText(errs))
			}

			if rendered := renderNode(got); rendered != tc.want {
				t.Fatalf("ParseExpr(%q) = %s, want %s", tc.input, rendered, tc.want)
			}
		})
	}
}

func TestParseExprReportsTrailingTokens(t *testing.T) {
	t.Parallel()

	_, errs := parseExprString("1 2")
	if len(errs) != 1 {
		t.Fatalf("len(errors) = %d, want 1", len(errs))
	}

	if got, want := errs[0].Error(), `ERROR [SQLSTATE 42601] at 1:3 (offset 2): expected end of expression, found numeric literal "2"`; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestParseExprReportsMissingCaseWhen(t *testing.T) {
	t.Parallel()

	_, errs := parseExprString("CASE 1 END")
	if len(errs) != 1 {
		t.Fatalf("len(errors) = %d, want 1", len(errs))
	}

	if got, want := errs[0].Error(), "ERROR [SQLSTATE 42601] at 1:8 (offset 7): expected WHEN in CASE expression"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestParseExprRejectsInvalidStarForms(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		input  string
		errors []string
	}{
		{
			name:   "bare star",
			input:  "*",
			errors: []string{"ERROR [SQLSTATE 42601] at 1:1 (offset 0): bare * is not a valid scalar expression"},
		},
		{
			name:   "abs star",
			input:  "ABS(*)",
			errors: []string{"ERROR [SQLSTATE 42601] at 1:5 (offset 4): * is only valid in COUNT(*)"},
		},
		{
			name:   "distinct star",
			input:  "COUNT(DISTINCT *)",
			errors: []string{"ERROR [SQLSTATE 42601] at 1:16 (offset 15): DISTINCT cannot be used with * in function call"},
		},
		{
			name:   "all star",
			input:  "COUNT(ALL *)",
			errors: []string{"ERROR [SQLSTATE 42601] at 1:11 (offset 10): ALL cannot be used with * in function call"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, errs := parseExprString(tc.input)
			if got := errorsText(errs); !equalStrings(got, tc.errors) {
				t.Fatalf("errors = %v, want %v", got, tc.errors)
			}
		})
	}
}

func TestParseTypeNameStopsBeforeConstraintKeywords(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		input    string
		wantName string
		wantNext string
	}{
		{
			name:     "default",
			input:    "INTEGER DEFAULT 1",
			wantName: "name(integer)",
			wantNext: `keyword "DEFAULT"`,
		},
		{
			name:     "not null",
			input:    "CHARACTER VARYING NOT NULL",
			wantName: "name(character.varying)",
			wantNext: `keyword "NOT"`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := New(lexer.NewString(tc.input).All())
			got := p.parseTypeName()

			if rendered := renderNode(got); rendered != tc.wantName {
				t.Fatalf("parseTypeName(%q) = %s, want %s", tc.input, rendered, tc.wantName)
			}

			if tok := p.peekSignificant(); describeToken(tok) != tc.wantNext {
				t.Fatalf("next token = %s, want %s", describeToken(tok), tc.wantNext)
			}
		})
	}
}

func TestMalformedDottedParsingPreservesSemicolon(t *testing.T) {
	t.Parallel()

	t.Run("qualified name", func(t *testing.T) {
		t.Parallel()

		p := New(lexer.NewString("owner.;").All())
		got := p.parseQualifiedName(false)

		if rendered := renderNode(got); rendered != "name(owner)" {
			t.Fatalf("parseQualifiedName() = %s, want %s", rendered, "name(owner)")
		}

		if tok := p.peekSignificant(); tok.Kind != token.KindPunctuation || tok.Lexeme != ";" {
			t.Fatalf("peek after parseQualifiedName = %v, want semicolon", tok)
		}

		if gotErrors := errorsText(diagnosticsToErrors(p.Errors())); !equalStrings(gotErrors, []string{`ERROR [SQLSTATE 42601] at 1:7 (offset 6): expected identifier, found punctuation ";"`}) {
			t.Fatalf("errors = %v", gotErrors)
		}
	})

	t.Run("type name", func(t *testing.T) {
		t.Parallel()

		p := New(lexer.NewString("owner.;").All())
		got := p.parseTypeName()

		if rendered := renderNode(got); rendered != "name(owner)" {
			t.Fatalf("parseTypeName() = %s, want %s", rendered, "name(owner)")
		}

		if tok := p.peekSignificant(); tok.Kind != token.KindPunctuation || tok.Lexeme != ";" {
			t.Fatalf("peek after parseTypeName = %v, want semicolon", tok)
		}

		if gotErrors := errorsText(diagnosticsToErrors(p.Errors())); !equalStrings(gotErrors, []string{`ERROR [SQLSTATE 42601] at 1:7 (offset 6): expected identifier, found punctuation ";"`}) {
			t.Fatalf("errors = %v", gotErrors)
		}
	})
}

func TestParseScriptSelectStatements(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "all core clauses",
			input: "SELECT DISTINCT customer_id AS cid, COUNT(*) FROM sales.orders AS o WHERE amount > 0 GROUP BY customer_id HAVING COUNT(*) > 1 ORDER BY customer_id DESC",
			want:  "script([select(DISTINCT [item(id(customer_id) AS id(cid)), item(call(name(count), [star(*)]))] FROM [from(name(sales.orders) AS id(o))] WHERE (id(amount) > int(0)) GROUP [id(customer_id)] HAVING (call(name(count), [star(*)]) > int(1)) ORDER [order(id(customer_id) DESC)])])",
		},
		{
			name:  "join and select star",
			input: "SELECT *, o.* FROM orders o INNER JOIN customers c ON o.customer_id = c.id",
			want:  "script([select([item(star(*)), item(star(name(o).*))] FROM [join(INNER, from(name(orders) AS id(o)), from(name(customers) AS id(c)), on=(name(o.customer_id) = name(c.id)))])])",
		},
		{
			name:  "derived table",
			input: "SELECT q.id FROM (SELECT id FROM orders) AS q",
			want:  "script([select([item(name(q.id))] FROM [from(select([item(id(id))] FROM [from(name(orders))]) AS id(q))])])",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			script, errs := parseScriptString(tc.input)
			if len(errs) != 0 {
				t.Fatalf("Parse(%q) errors = %v, want none", tc.input, errorsText(errs))
			}

			if got := renderNode(script); got != tc.want {
				t.Fatalf("Parse(%q) = %s, want %s", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseScriptDMLStatements(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "insert values",
			input: "INSERT INTO orders (id, total) VALUES (1, 2), (3, 4)",
			want:  "script([insert(name(orders), cols=[id(id), id(total)], values([[int(1), int(2)], [int(3), int(4)]]))])",
		},
		{
			name:  "insert select",
			input: "INSERT INTO archive SELECT * FROM orders",
			want:  "script([insert(name(archive), query(select([item(star(*))] FROM [from(name(orders))])))])",
		},
		{
			name:  "insert default values",
			input: "INSERT INTO defaults DEFAULT VALUES",
			want:  "script([insert(name(defaults), default values)])",
		},
		{
			name:  "update with tuple assignment",
			input: "UPDATE orders SET total = 1, (qty, price) = (2, 3) WHERE id = 9",
			want:  "script([update(name(orders), [assign(id(total) = int(1)), assign([id(qty), id(price)] = [int(2), int(3)])], where=(id(id) = int(9)))])",
		},
		{
			name:  "delete",
			input: "DELETE FROM orders WHERE id = 9",
			want:  "script([delete(name(orders), where=(id(id) = int(9)))])",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			script, errs := parseScriptString(tc.input)
			if len(errs) != 0 {
				t.Fatalf("Parse(%q) errors = %v, want none", tc.input, errorsText(errs))
			}

			if got := renderNode(script); got != tc.want {
				t.Fatalf("Parse(%q) = %s, want %s", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseScriptDDLStatements(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "create table",
			input: "CREATE TABLE orders (" +
				"id INTEGER PRIMARY KEY, " +
				"code CHARACTER VARYING(12) DEFAULT 'x' NOT NULL, " +
				"parent_id INTEGER REFERENCES parents(id), " +
				"CONSTRAINT uq UNIQUE (code), " +
				"FOREIGN KEY (parent_id) REFERENCES parents(id))",
			want: "script([create table(name(orders), columns=[column(id(id), name(integer), constraints=[constraint(PRIMARY KEY)]), column(id(code), name(character.varying)([int(12)]), default=string(\"x\"), constraints=[constraint(NOT NULL)]), column(id(parent_id), name(integer), constraints=[constraint(REFERENCES ref=references(name(parents), cols=[id(id)]))])], constraints=[constraint(id(uq) UNIQUE cols=[id(code)]), constraint(FOREIGN KEY cols=[id(parent_id)] ref=references(name(parents), cols=[id(id)]))])])",
		},
		{
			name:  "drop table",
			input: "DROP TABLE orders",
			want:  "script([drop table(name(orders))])",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			script, errs := parseScriptString(tc.input)
			if len(errs) != 0 {
				t.Fatalf("Parse(%q) errors = %v, want none", tc.input, errorsText(errs))
			}

			if got := renderNode(script); got != tc.want {
				t.Fatalf("Parse(%q) = %s, want %s", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseScriptTransactions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "begin", input: "BEGIN", want: "script([begin])"},
		{name: "begin transaction", input: "BEGIN TRANSACTION", want: "script([begin TRANSACTION])"},
		{name: "transaction sequence", input: "BEGIN WORK; COMMIT WORK; ROLLBACK;", want: "script([begin WORK, commit WORK, rollback])"},
		{name: "comments ignored", input: "BEGIN /* x */; -- y\nCOMMIT;", want: "script([begin, commit])"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			script, errs := parseScriptString(tc.input)
			if len(errs) != 0 {
				t.Fatalf("Parse(%q) errors = %v, want none", tc.input, errorsText(errs))
			}

			if got := renderNode(script); got != tc.want {
				t.Fatalf("Parse(%q) = %s, want %s", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseScriptSkipsEmptyStatements(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "leading and repeated semicolons", input: "; BEGIN;; COMMIT;;", want: "script([begin, commit])"},
		{name: "comments between empty statements", input: "BEGIN; /* empty */ ; -- still empty\n; COMMIT;;", want: "script([begin, commit])"},
		{name: "only semicolons", input: ";;; ", want: "script([])"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			script, errs := parseScriptString(tc.input)
			if len(errs) != 0 {
				t.Fatalf("Parse(%q) errors = %v, want none", tc.input, errorsText(errs))
			}

			if got := renderNode(script); got != tc.want {
				t.Fatalf("Parse(%q) = %s, want %s", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseScriptRecoversAfterBadStatement(t *testing.T) {
	t.Parallel()

	script, errs := parseScriptString("BEGIN; UPSERT widgets; COMMIT;")
	if len(errs) != 1 {
		t.Fatalf("len(errors) = %d, want 1", len(errs))
	}

	if got, want := renderNode(script), "script([begin, commit])"; got != want {
		t.Fatalf("renderNode(script) = %s, want %s", got, want)
	}

	if got, want := errs[0].Error(), `ERROR [SQLSTATE 42601] at 1:8 (offset 7): expected BEGIN, COMMIT, ROLLBACK, SELECT, INSERT, UPDATE, DELETE, CREATE, or DROP, found identifier "UPSERT"`; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestParseScriptRecoversAfterBadStatementWithEmptySeparators(t *testing.T) {
	t.Parallel()

	script, errs := parseScriptString("BEGIN;; UPSERT widgets; ; COMMIT;;")
	if len(errs) != 1 {
		t.Fatalf("len(errors) = %d, want 1", len(errs))
	}

	if got, want := renderNode(script), "script([begin, commit])"; got != want {
		t.Fatalf("renderNode(script) = %s, want %s", got, want)
	}

	if got, want := errs[0].Error(), `ERROR [SQLSTATE 42601] at 1:9 (offset 8): expected BEGIN, COMMIT, ROLLBACK, SELECT, INSERT, UPDATE, DELETE, CREATE, or DROP, found identifier "UPSERT"`; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestParseScriptRecoversAfterLexerError(t *testing.T) {
	t.Parallel()

	script, errs := parseScriptString("BEGIN; @; COMMIT;")
	if len(errs) != 1 {
		t.Fatalf("len(errors) = %d, want 1", len(errs))
	}

	if got, want := renderNode(script), "script([begin, commit])"; got != want {
		t.Fatalf("renderNode(script) = %s, want %s", got, want)
	}

	if got, want := errs[0].Error(), "ERROR [SQLSTATE 42601] at 1:8 (offset 7): unexpected character"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func testToken(kind token.Kind, lexeme string, start token.Pos) token.Token {
	end := token.Pos{
		Line:   start.Line,
		Column: start.Column + len(lexeme),
		Offset: start.Offset + len(lexeme),
	}

	return token.Token{
		Kind:   kind,
		Span:   token.Span{Start: start, Stop: end},
		Lexeme: lexeme,
		Text:   lexeme,
	}
}

func parseExprString(input string) (Node, []error) {
	p := New(lexer.NewString(input).All())
	return p.ParseExpr(), diagnosticsToErrors(p.Errors())
}

func parseScriptString(input string) (*Script, []error) {
	p := New(lexer.NewString(input).All())
	return p.Parse(), diagnosticsToErrors(p.Errors())
}

func diagnosticsToErrors(diags []diag.Diagnostic) []error {
	errs := make([]error, 0, len(diags))
	for _, diag := range diags {
		errs = append(errs, fmt.Errorf("%s", diag.Error()))
	}

	return errs
}

func errorsText(errs []error) []string {
	lines := make([]string, 0, len(errs))
	for _, err := range errs {
		lines = append(lines, err.Error())
	}

	return lines
}

func renderNode(node Node) string {
	switch node := node.(type) {
	case nil:
		return "<nil>"
	case *Script:
		items := make([]string, 0, len(node.Nodes))
		for _, item := range node.Nodes {
			items = append(items, renderNode(item))
		}
		return fmt.Sprintf("script([%s])", strings.Join(items, ", "))
	case *Identifier:
		if node.Quoted {
			return fmt.Sprintf("qid(%q)", node.Name)
		}
		return fmt.Sprintf("id(%s)", node.Name)
	case *QualifiedName:
		parts := make([]string, 0, len(node.Parts))
		for _, part := range node.Parts {
			parts = append(parts, part.Name)
		}
		return fmt.Sprintf("name(%s)", strings.Join(parts, "."))
	case *Star:
		if node.Qualifier == nil {
			return "star(*)"
		}
		return fmt.Sprintf("star(%s.*)", renderNode(node.Qualifier))
	case *IntegerLiteral:
		return fmt.Sprintf("int(%s)", node.Text)
	case *FloatLiteral:
		return fmt.Sprintf("float(%s)", node.Text)
	case *StringLiteral:
		return fmt.Sprintf("string(%q)", node.Value)
	case *BoolLiteral:
		return fmt.Sprintf("bool(%t)", node.Value)
	case *NullLiteral:
		return "null"
	case *ParamLiteral:
		return fmt.Sprintf("param(%s)", node.Text)
	case *UnaryExpr:
		return fmt.Sprintf("(%s %s)", node.Operator, renderNode(node.Operand))
	case *BinaryExpr:
		return fmt.Sprintf("(%s %s %s)", renderNode(node.Left), node.Operator, renderNode(node.Right))
	case *FunctionCall:
		args := make([]string, 0, len(node.Args))
		for _, arg := range node.Args {
			args = append(args, renderNode(arg))
		}
		if node.SetQuantifier != "" {
			return fmt.Sprintf("call(%s, %s [%s])", renderNode(node.Name), node.SetQuantifier, strings.Join(args, ", "))
		}
		return fmt.Sprintf("call(%s, [%s])", renderNode(node.Name), strings.Join(args, ", "))
	case *CastExpr:
		return fmt.Sprintf("cast(%s AS %s)", renderNode(node.Expr), renderNode(node.Type))
	case *WhenClause:
		return fmt.Sprintf("when(%s => %s)", renderNode(node.Condition), renderNode(node.Result))
	case *CaseExpr:
		whens := make([]string, 0, len(node.Whens))
		for _, when := range node.Whens {
			whens = append(whens, renderNode(when))
		}
		if node.Operand != nil {
			return fmt.Sprintf("case(%s, [%s], else=%s)", renderNode(node.Operand), strings.Join(whens, ", "), renderNode(node.Else))
		}
		return fmt.Sprintf("case([%s], else=%s)", strings.Join(whens, ", "), renderNode(node.Else))
	case *BetweenExpr:
		prefix := "between"
		if node.Negated {
			prefix = "not between"
		}
		return fmt.Sprintf("%s(%s, %s, %s)", prefix, renderNode(node.Expr), renderNode(node.Lower), renderNode(node.Upper))
	case *InExpr:
		items := make([]string, 0, len(node.List))
		for _, item := range node.List {
			items = append(items, renderNode(item))
		}
		prefix := "in"
		if node.Negated {
			prefix = "not in"
		}
		return fmt.Sprintf("%s(%s, [%s])", prefix, renderNode(node.Expr), strings.Join(items, ", "))
	case *LikeExpr:
		prefix := "like"
		if node.Negated {
			prefix = "not like"
		}
		if node.Escape != nil {
			return fmt.Sprintf("%s(%s, %s, %s)", prefix, renderNode(node.Expr), renderNode(node.Pattern), renderNode(node.Escape))
		}
		return fmt.Sprintf("%s(%s, %s)", prefix, renderNode(node.Expr), renderNode(node.Pattern))
	case *IsExpr:
		if node.Right != nil {
			if node.Negated {
				return fmt.Sprintf("is(%s NOT %s %s)", renderNode(node.Expr), node.Predicate, renderNode(node.Right))
			}
			return fmt.Sprintf("is(%s %s %s)", renderNode(node.Expr), node.Predicate, renderNode(node.Right))
		}
		if node.Negated {
			return fmt.Sprintf("is(%s NOT %s)", renderNode(node.Expr), node.Predicate)
		}
		return fmt.Sprintf("is(%s %s)", renderNode(node.Expr), node.Predicate)
	case *TypeName:
		parts := make([]string, 0, len(node.Names))
		if node.Qualifier != nil {
			for _, part := range node.Qualifier.Parts {
				parts = append(parts, part.Name)
			}
		}
		for _, part := range node.Names {
			parts = append(parts, part.Name)
		}
		rendered := fmt.Sprintf("name(%s)", strings.Join(parts, "."))
		if len(node.Args) == 0 {
			return rendered
		}
		args := make([]string, 0, len(node.Args))
		for _, arg := range node.Args {
			args = append(args, renderNode(arg))
		}
		return fmt.Sprintf("%s([%s])", rendered, strings.Join(args, ", "))
	case *SelectStmt:
		parts := []string{fmt.Sprintf("[%s]", renderSelectItems(node.SelectList))}
		if node.SetQuantifier != "" {
			parts[0] = node.SetQuantifier + " " + parts[0]
		}
		if len(node.From) > 0 {
			parts = append(parts, fmt.Sprintf("FROM [%s]", renderNodeSlice(node.From)))
		}
		if node.Where != nil {
			parts = append(parts, "WHERE "+renderNode(node.Where))
		}
		if len(node.GroupBy) > 0 {
			parts = append(parts, fmt.Sprintf("GROUP [%s]", renderNodeSlice(node.GroupBy)))
		}
		if node.Having != nil {
			parts = append(parts, "HAVING "+renderNode(node.Having))
		}
		if len(node.OrderBy) > 0 {
			parts = append(parts, fmt.Sprintf("ORDER [%s]", renderOrderByItems(node.OrderBy)))
		}
		return fmt.Sprintf("select(%s)", strings.Join(parts, " "))
	case *SelectItem:
		if node.Alias != nil {
			return fmt.Sprintf("item(%s AS %s)", renderNode(node.Expr), renderNode(node.Alias))
		}
		return fmt.Sprintf("item(%s)", renderNode(node.Expr))
	case *FromSource:
		if node.Alias != nil {
			return fmt.Sprintf("from(%s AS %s)", renderNode(node.Source), renderNode(node.Alias))
		}
		return fmt.Sprintf("from(%s)", renderNode(node.Source))
	case *JoinExpr:
		kind := node.Type
		if node.Natural {
			kind = "NATURAL " + kind
		}
		parts := []string{kind, renderNode(node.Left), renderNode(node.Right)}
		if len(node.Using) > 0 {
			parts = append(parts, fmt.Sprintf("using=[%s]", renderIdentifierSlice(node.Using)))
		}
		if node.Condition != nil {
			parts = append(parts, "on="+renderNode(node.Condition))
		}
		return fmt.Sprintf("join(%s)", strings.Join(parts, ", "))
	case *OrderByItem:
		if node.Direction != "" {
			return fmt.Sprintf("order(%s %s)", renderNode(node.Expr), node.Direction)
		}
		return fmt.Sprintf("order(%s)", renderNode(node.Expr))
	case *InsertValuesSource:
		rows := make([]string, 0, len(node.Rows))
		for _, row := range node.Rows {
			rows = append(rows, fmt.Sprintf("[%s]", renderNodeSlice(row)))
		}
		return fmt.Sprintf("values([%s])", strings.Join(rows, ", "))
	case *InsertQuerySource:
		return fmt.Sprintf("query(%s)", renderNode(node.Query))
	case *InsertDefaultValuesSource:
		return "default values"
	case *InsertStmt:
		parts := []string{renderNode(node.Table)}
		if len(node.Columns) > 0 {
			parts = append(parts, fmt.Sprintf("cols=[%s]", renderIdentifierSlice(node.Columns)))
		}
		if node.Source != nil {
			parts = append(parts, renderNode(node.Source))
		}
		return fmt.Sprintf("insert(%s)", strings.Join(parts, ", "))
	case *UpdateAssignment:
		if len(node.Columns) == 1 && len(node.Values) == 1 {
			return fmt.Sprintf("assign(%s = %s)", renderNode(node.Columns[0]), renderNode(node.Values[0]))
		}
		return fmt.Sprintf("assign([%s] = [%s])", renderIdentifierSlice(node.Columns), renderNodeSlice(node.Values))
	case *UpdateStmt:
		parts := []string{renderNode(node.Table), fmt.Sprintf("[%s]", renderAssignments(node.Assignments))}
		if node.Where != nil {
			parts = append(parts, "where="+renderNode(node.Where))
		}
		return fmt.Sprintf("update(%s)", strings.Join(parts, ", "))
	case *DeleteStmt:
		if node.Where != nil {
			return fmt.Sprintf("delete(%s, where=%s)", renderNode(node.Table), renderNode(node.Where))
		}
		return fmt.Sprintf("delete(%s)", renderNode(node.Table))
	case *ReferenceSpec:
		if len(node.Columns) > 0 {
			return fmt.Sprintf("references(%s, cols=[%s])", renderNode(node.Table), renderIdentifierSlice(node.Columns))
		}
		return fmt.Sprintf("references(%s)", renderNode(node.Table))
	case *ConstraintDef:
		parts := make([]string, 0, 4)
		if node.Name != nil {
			parts = append(parts, renderNode(node.Name))
		}
		parts = append(parts, string(node.Kind))
		if len(node.Columns) > 0 {
			parts = append(parts, fmt.Sprintf("cols=[%s]", renderIdentifierSlice(node.Columns)))
		}
		if node.Check != nil {
			parts = append(parts, "check="+renderNode(node.Check))
		}
		if node.Reference != nil {
			parts = append(parts, "ref="+renderNode(node.Reference))
		}
		return fmt.Sprintf("constraint(%s)", strings.Join(parts, " "))
	case *ColumnDef:
		parts := []string{renderNode(node.Name)}
		if node.Type != nil {
			parts = append(parts, renderNode(node.Type))
		}
		if node.Default != nil {
			parts = append(parts, "default="+renderNode(node.Default))
		}
		if len(node.Constraints) > 0 {
			parts = append(parts, fmt.Sprintf("constraints=[%s]", renderConstraints(node.Constraints)))
		}
		return fmt.Sprintf("column(%s)", strings.Join(parts, ", "))
	case *CreateTableStmt:
		parts := []string{renderNode(node.Name)}
		if len(node.Columns) > 0 {
			parts = append(parts, fmt.Sprintf("columns=[%s]", renderColumns(node.Columns)))
		}
		if len(node.Constraints) > 0 {
			parts = append(parts, fmt.Sprintf("constraints=[%s]", renderConstraints(node.Constraints)))
		}
		return fmt.Sprintf("create table(%s)", strings.Join(parts, ", "))
	case *DropTableStmt:
		return fmt.Sprintf("drop table(%s)", renderNode(node.Name))
	case *BeginStmt:
		if node.Mode == "" {
			return "begin"
		}
		return "begin " + node.Mode
	case *CommitStmt:
		if node.Work {
			return "commit WORK"
		}
		return "commit"
	case *RollbackStmt:
		if node.Work {
			return "rollback WORK"
		}
		return "rollback"
	default:
		return fmt.Sprintf("%T", node)
	}
}

func renderNodeSlice(items []Node) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, renderNode(item))
	}

	return strings.Join(parts, ", ")
}

func renderIdentifierSlice(items []*Identifier) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, renderNode(item))
	}

	return strings.Join(parts, ", ")
}

func renderSelectItems(items []*SelectItem) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, renderNode(item))
	}

	return strings.Join(parts, ", ")
}

func renderOrderByItems(items []*OrderByItem) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, renderNode(item))
	}

	return strings.Join(parts, ", ")
}

func renderAssignments(items []*UpdateAssignment) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, renderNode(item))
	}

	return strings.Join(parts, ", ")
}

func renderConstraints(items []*ConstraintDef) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, renderNode(item))
	}

	return strings.Join(parts, ", ")
}

func renderColumns(items []*ColumnDef) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, renderNode(item))
	}

	return strings.Join(parts, ", ")
}

func equalStrings(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}

	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}

	return true
}
