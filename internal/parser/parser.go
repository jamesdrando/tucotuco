package parser

import (
	"fmt"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/token"
)

const syntaxSQLState = "42601"

// Parser provides cursor-style access over a token stream.
//
// The raw Peek/Consume methods intentionally preserve the original scaffold
// behavior. Parse helpers use comment-skipping lookahead on top of that cursor
// so expression and statement parsing can ignore trivia without changing the
// existing token-stream contract.
type Parser struct {
	tokens []token.Token
	index  int
	errors []diag.Diagnostic
}

// New constructs a parser over a token slice.
//
// The input slice is copied so callers can retain ownership of their token
// buffers. Parsing stops at the first EOF token, and a single EOF sentinel is
// always present at the end so cursor operations have a stable end-of-stream
// marker.
func New(tokens []token.Token) *Parser {
	cloned := make([]token.Token, 0, len(tokens)+1)
	for _, tok := range tokens {
		cloned = append(cloned, tok)
		if tok.Kind == token.KindEOF {
			break
		}
	}
	if len(cloned) == 0 || cloned[len(cloned)-1].Kind != token.KindEOF {
		cloned = append(cloned, eofToken(cloned))
	}

	return &Parser{
		tokens: cloned,
	}
}

// Peek returns the current token without advancing the cursor.
func (p *Parser) Peek() token.Token {
	return p.current()
}

// Consume returns the current token and advances the cursor by one position.
func (p *Parser) Consume() token.Token {
	tok := p.current()
	if p.index < len(p.tokens) {
		p.index++
	}

	return tok
}

// Parse parses the current token stream into the parser-local CST.
func (p *Parser) Parse() *Script {
	return p.ParseScript()
}

// ParseScript parses semicolon-delimited SQL statements into the parser-local
// CST.
func (p *Parser) ParseScript() *Script {
	start := p.peekSignificant().Pos()
	if start.IsZero() {
		start = token.Pos{Line: 1, Column: 1, Offset: 0}
	}

	nodes := make([]Node, 0, 4)
	for {
		if p.consumePunctuation(";") {
			continue
		}

		tok := p.peekSignificant()
		if tok.Kind == token.KindEOF {
			return &Script{
				Span: token.Span{
					Start: start,
					Stop:  tok.End(),
				},
				Nodes: nodes,
			}
		}

		stmt, ok := p.parseStatement()
		if !ok {
			p.recoverStatement()
			continue
		}
		if stmt != nil {
			nodes = append(nodes, stmt)
		}

		if p.consumePunctuation(";") {
			continue
		}

		tok = p.peekSignificant()
		if tok.Kind == token.KindEOF {
			continue
		}

		p.addError(tok, fmt.Sprintf("expected semicolon or end of input, found %s", describeToken(tok)))
		p.recoverStatement()
	}
}

// ParseExpr parses one scalar expression into the parser-local CST.
func (p *Parser) ParseExpr() Node {
	expr := p.parseExpr()
	tok := p.peekSignificant()
	if tok.Kind == token.KindEOF {
		return expr
	}
	if tok.Kind == token.KindPunctuation && tok.Lexeme == ";" {
		return expr
	}

	p.addError(tok, fmt.Sprintf("expected end of expression, found %s", describeToken(tok)))

	return expr
}

// Errors returns the diagnostics recorded so far.
func (p *Parser) Errors() []diag.Diagnostic {
	if len(p.errors) == 0 {
		return nil
	}

	out := make([]diag.Diagnostic, len(p.errors))
	copy(out, p.errors)

	return out
}

func (p *Parser) current() token.Token {
	if len(p.tokens) == 0 {
		return eofToken(nil)
	}

	if p.index >= len(p.tokens) {
		return p.tokens[len(p.tokens)-1]
	}

	return p.tokens[p.index]
}

func (p *Parser) addError(tok token.Token, message string) {
	p.errors = append(p.errors, diag.NewError(syntaxSQLState, message, toDiagPosition(tok.Pos())))
}

func (p *Parser) recoverStatement() {
	for {
		tok := p.Peek()
		switch tok.Kind {
		case token.KindEOF:
			return
		case token.KindPunctuation:
			if tok.Lexeme == ";" {
				p.Consume()
				return
			}
		case token.KindError:
			message := tok.Message
			if message == "" {
				message = "syntax error"
			}
			p.addError(tok, message)
		}

		p.Consume()
	}
}

func (p *Parser) parseStatement() (Node, bool) {
	tok := p.peekSignificant()
	switch {
	case tok.Kind == token.KindError:
		p.consumeSignificant()
		message := tok.Message
		if message == "" {
			message = "syntax error"
		}
		p.addError(tok, message)

		return nil, false
	case tok.IsKeyword("BEGIN"):
		return p.parseBegin(), true
	case tok.IsKeyword("COMMIT"):
		return p.parseCommit(), true
	case tok.IsKeyword("ROLLBACK"):
		return p.parseRollback(), true
	case tok.IsKeyword("SELECT"):
		return p.parseSelect(), true
	case tok.IsKeyword("INSERT"):
		return p.parseInsert(), true
	case tok.IsKeyword("UPDATE"):
		return p.parseUpdate(), true
	case tok.IsKeyword("DELETE"):
		return p.parseDelete(), true
	case tok.IsKeyword("CREATE"):
		return p.parseCreate(), true
	case tok.IsKeyword("DROP"):
		return p.parseDrop(), true
	default:
		p.addError(tok, fmt.Sprintf("expected BEGIN, COMMIT, ROLLBACK, SELECT, INSERT, UPDATE, DELETE, CREATE, or DROP, found %s", describeToken(tok)))

		return nil, false
	}
}

func (p *Parser) parseBegin() *BeginStmt {
	begin := p.consumeSignificant()
	stmt := &BeginStmt{
		Span: token.Span{
			Start: begin.Pos(),
			Stop:  begin.End(),
		},
	}

	switch {
	case p.consumeKeyword("WORK"):
		stmt.Mode = "WORK"
		stmt.Stop = p.lastConsumed().End()
	case p.consumeKeyword("TRANSACTION"):
		stmt.Mode = "TRANSACTION"
		stmt.Stop = p.lastConsumed().End()
	}

	return stmt
}

func (p *Parser) parseCommit() *CommitStmt {
	commit := p.consumeSignificant()
	stmt := &CommitStmt{
		Span: token.Span{
			Start: commit.Pos(),
			Stop:  commit.End(),
		},
	}

	if p.consumeKeyword("WORK") {
		stmt.Work = true
		stmt.Stop = p.lastConsumed().End()
	}

	return stmt
}

func (p *Parser) parseRollback() *RollbackStmt {
	rollback := p.consumeSignificant()
	stmt := &RollbackStmt{
		Span: token.Span{
			Start: rollback.Pos(),
			Stop:  rollback.End(),
		},
	}

	if p.consumeKeyword("WORK") {
		stmt.Work = true
		stmt.Stop = p.lastConsumed().End()
	}

	return stmt
}

func (p *Parser) parseSelect() *SelectStmt {
	selectTok := p.consumeSignificant()
	stmt := &SelectStmt{
		Span: token.Span{
			Start: selectTok.Pos(),
			Stop:  selectTok.End(),
		},
	}

	if p.consumeKeyword("DISTINCT") {
		stmt.SetQuantifier = "DISTINCT"
		stmt.Stop = p.lastConsumed().End()
	} else if p.consumeKeyword("ALL") {
		stmt.SetQuantifier = "ALL"
		stmt.Stop = p.lastConsumed().End()
	}

	for {
		item := p.parseSelectItem()
		if item != nil {
			stmt.SelectList = append(stmt.SelectList, item)
			stmt.Stop = item.End()
		}

		if !p.consumePunctuation(",") {
			break
		}
	}

	if p.consumeKeyword("FROM") {
		for {
			before := p.index
			source := p.parseFromSource()
			if source != nil {
				stmt.From = append(stmt.From, source)
				stmt.Stop = source.End()
			}

			if p.consumePunctuation(",") {
				continue
			}
			if p.index == before {
				break
			}

			break
		}
	}

	if p.consumeKeyword("WHERE") {
		stmt.Where = p.parseExpr()
		stmt.Stop = spanFromNode(stmt.Where, stmt.Stop)
	}

	if p.consumeKeyword("GROUP") {
		p.expectKeyword("BY")
		for {
			tok := p.peekSignificant()
			if isGroupByBoundary(tok) {
				p.addError(tok, fmt.Sprintf("expected expression in GROUP BY, found %s", describeToken(tok)))
				break
			}

			expr := p.parseExpr()
			if expr != nil {
				stmt.GroupBy = append(stmt.GroupBy, expr)
				stmt.Stop = expr.End()
			}

			if !p.consumePunctuation(",") {
				break
			}
		}
	}

	if p.consumeKeyword("HAVING") {
		stmt.Having = p.parseExpr()
		stmt.Stop = spanFromNode(stmt.Having, stmt.Stop)
	}

	if p.consumeKeyword("ORDER") {
		p.expectKeyword("BY")
		for {
			item := p.parseOrderByItem()
			if item != nil {
				stmt.OrderBy = append(stmt.OrderBy, item)
				stmt.Stop = item.End()
			}

			if !p.consumePunctuation(",") {
				break
			}
		}
	}

	return stmt
}

func (p *Parser) parseSelectItem() *SelectItem {
	tok := p.peekSignificant()
	if isSelectClauseBoundary(tok) {
		p.addError(tok, fmt.Sprintf("expected select item, found %s", describeToken(tok)))
		return nil
	}

	var expr Node
	if p.matchOperator("*") {
		star := p.consumeSignificant()
		expr = &Star{Span: star.Span}
	} else {
		expr = p.parseExpr()
	}

	item := &SelectItem{
		Span: token.Span{
			Start: nodePos(expr),
			Stop:  nodeEnd(expr),
		},
		Expr: expr,
	}

	if p.consumeKeyword("AS") {
		item.Alias = p.parseIdentifierToken(false)
		item.Stop = spanFromNode(item.Alias, item.Stop)
		return item
	}

	if alias := p.parseOptionalAlias(); alias != nil {
		item.Alias = alias
		item.Stop = alias.End()
	}

	return item
}

func (p *Parser) parseFromSource() Node {
	tok := p.peekSignificant()
	if isFromClauseBoundary(tok) {
		p.addError(tok, fmt.Sprintf("expected table source, found %s", describeToken(tok)))
		return nil
	}

	left := p.parseFromPrimary()
	for {
		joinType, natural, ok := p.parseJoinType()
		if !ok {
			return left
		}

		right := p.parseFromPrimary()
		join := &JoinExpr{
			Span: token.Span{
				Start: nodePos(left),
				Stop:  spanFromNode(right, nodeEnd(left)),
			},
			Left:    left,
			Right:   right,
			Type:    joinType,
			Natural: natural,
		}

		if natural || joinType == "CROSS" {
			left = join
			continue
		}

		if p.consumeKeyword("ON") {
			join.Condition = p.parseExpr()
			join.Stop = spanFromNode(join.Condition, join.Stop)
			left = join
			continue
		}

		if p.consumeKeyword("USING") {
			columns, stop := p.parseParenthesizedIdentifierList("JOIN USING")
			join.Using = columns
			join.Stop = stop
			left = join
			continue
		}

		tok := p.peekSignificant()
		p.addError(tok, fmt.Sprintf("expected ON or USING, found %s", describeToken(tok)))
		left = join
	}
}

func (p *Parser) parseFromPrimary() Node {
	tok := p.peekSignificant()
	if isFromClauseBoundary(tok) {
		p.addError(tok, fmt.Sprintf("expected table source, found %s", describeToken(tok)))
		return nil
	}

	if p.matchPunctuation("(") {
		open := p.consumeSignificant()

		var source Node
		if p.matchKeyword("SELECT") {
			source = p.parseSelect()
		} else {
			source = p.parseFromSource()
		}

		closeTok := p.expectPunctuation(")")
		item := &FromSource{
			Span: token.Span{
				Start: open.Pos(),
				Stop:  spanEnd(closeTok, spanFromNode(source, open.End())),
			},
			Source: source,
		}

		if p.consumeKeyword("AS") {
			item.Alias = p.parseIdentifierToken(false)
			item.Stop = spanFromNode(item.Alias, item.Stop)
			return item
		}

		if alias := p.parseOptionalAlias(); alias != nil {
			item.Alias = alias
			item.Stop = alias.End()
		}

		return item
	}

	name := p.parseQualifiedName(false)
	if name == nil {
		return nil
	}

	item := &FromSource{
		Span: token.Span{
			Start: name.Pos(),
			Stop:  name.End(),
		},
		Source: name,
	}

	if p.consumeKeyword("AS") {
		item.Alias = p.parseIdentifierToken(false)
		item.Stop = spanFromNode(item.Alias, item.Stop)
		return item
	}

	if alias := p.parseOptionalAlias(); alias != nil {
		item.Alias = alias
		item.Stop = alias.End()
	}

	return item
}

func (p *Parser) parseJoinType() (string, bool, bool) {
	natural := p.consumeKeyword("NATURAL")

	switch {
	case p.consumeKeyword("JOIN"):
		return "INNER", natural, true
	case p.consumeKeyword("INNER"):
		p.expectKeyword("JOIN")
		return "INNER", natural, true
	case p.consumeKeyword("LEFT"):
		p.consumeKeyword("OUTER")
		p.expectKeyword("JOIN")
		return "LEFT", natural, true
	case p.consumeKeyword("RIGHT"):
		p.consumeKeyword("OUTER")
		p.expectKeyword("JOIN")
		return "RIGHT", natural, true
	case p.consumeKeyword("FULL"):
		p.consumeKeyword("OUTER")
		p.expectKeyword("JOIN")
		return "FULL", natural, true
	case p.consumeKeyword("CROSS"):
		p.expectKeyword("JOIN")
		if natural {
			p.addError(p.lastConsumed(), "NATURAL cannot be combined with CROSS JOIN")
		}
		return "CROSS", natural, true
	default:
		if natural {
			p.expectKeyword("JOIN")
			return "INNER", true, true
		}
		return "", false, false
	}
}

func (p *Parser) parseOrderByItem() *OrderByItem {
	tok := p.peekSignificant()
	if isOrderByBoundary(tok) {
		p.addError(tok, fmt.Sprintf("expected ORDER BY expression, found %s", describeToken(tok)))
		return nil
	}

	expr := p.parseExpr()
	item := &OrderByItem{
		Span: token.Span{
			Start: nodePos(expr),
			Stop:  nodeEnd(expr),
		},
		Expr: expr,
	}

	if p.consumeKeyword("ASC") {
		item.Direction = "ASC"
		item.Stop = p.lastConsumed().End()
	} else if p.consumeKeyword("DESC") {
		item.Direction = "DESC"
		item.Stop = p.lastConsumed().End()
	}

	return item
}

func (p *Parser) parseInsert() *InsertStmt {
	insertTok := p.consumeSignificant()
	stmt := &InsertStmt{
		Span: token.Span{
			Start: insertTok.Pos(),
			Stop:  insertTok.End(),
		},
	}

	p.expectKeyword("INTO")
	stmt.Table = p.parseQualifiedName(false)
	stmt.Stop = spanFromNode(stmt.Table, stmt.Stop)

	if p.matchPunctuation("(") {
		columns, stop := p.parseParenthesizedIdentifierList("INSERT column list")
		stmt.Columns = columns
		stmt.Stop = stop
	}

	switch {
	case p.consumeKeyword("VALUES"):
		valuesTok := p.lastConsumed()
		source := &InsertValuesSource{
			Span: token.Span{
				Start: valuesTok.Pos(),
				Stop:  valuesTok.End(),
			},
		}

		for {
			row, stop := p.parseParenthesizedExprList("VALUES row")
			source.Rows = append(source.Rows, row)
			source.Stop = stop

			if !p.consumePunctuation(",") {
				break
			}
		}

		stmt.Source = source
	case p.consumeKeyword("DEFAULT"):
		defaultTok := p.lastConsumed()
		valuesTok := p.expectKeyword("VALUES")
		stmt.Source = &InsertDefaultValuesSource{
			Span: token.Span{
				Start: defaultTok.Pos(),
				Stop:  spanEnd(valuesTok, defaultTok.End()),
			},
		}
	case p.matchKeyword("SELECT"):
		query := p.parseSelect()
		stmt.Source = &InsertQuerySource{
			Span: token.Span{
				Start: nodePos(query),
				Stop:  nodeEnd(query),
			},
			Query: query,
		}
	default:
		tok := p.peekSignificant()
		p.addError(tok, fmt.Sprintf("expected VALUES, DEFAULT VALUES, or SELECT, found %s", describeToken(tok)))
	}

	stmt.Stop = spanFromNode(stmt.Source, stmt.Stop)

	return stmt
}

func (p *Parser) parseUpdate() *UpdateStmt {
	updateTok := p.consumeSignificant()
	stmt := &UpdateStmt{
		Span: token.Span{
			Start: updateTok.Pos(),
			Stop:  updateTok.End(),
		},
	}

	stmt.Table = p.parseQualifiedName(false)
	stmt.Stop = spanFromNode(stmt.Table, stmt.Stop)

	p.expectKeyword("SET")
	for {
		assignment := p.parseUpdateAssignment()
		if assignment != nil {
			stmt.Assignments = append(stmt.Assignments, assignment)
			stmt.Stop = assignment.End()
		}

		if !p.consumePunctuation(",") {
			break
		}
	}

	if p.consumeKeyword("WHERE") {
		stmt.Where = p.parseExpr()
		stmt.Stop = spanFromNode(stmt.Where, stmt.Stop)
	}

	return stmt
}

func (p *Parser) parseUpdateAssignment() *UpdateAssignment {
	stmt := &UpdateAssignment{}

	if p.matchPunctuation("(") {
		columns, stop := p.parseParenthesizedIdentifierList("assignment target list")
		stmt.Columns = columns
		stmt.Start = firstIdentifierPos(columns)
		stmt.Stop = stop

		p.expectOperator("=")
		values, stop := p.parseParenthesizedExprList("assignment value list")
		stmt.Values = values
		stmt.Stop = stop

		return stmt
	}

	column := p.parseIdentifierToken(false)
	if column == nil {
		return nil
	}

	stmt.Columns = []*Identifier{column}
	stmt.Start = column.Pos()
	stmt.Stop = column.End()

	p.expectOperator("=")
	value := p.parseExpr()
	if value != nil {
		stmt.Values = []Node{value}
		stmt.Stop = value.End()
	}

	return stmt
}

func (p *Parser) parseDelete() *DeleteStmt {
	deleteTok := p.consumeSignificant()
	stmt := &DeleteStmt{
		Span: token.Span{
			Start: deleteTok.Pos(),
			Stop:  deleteTok.End(),
		},
	}

	p.expectKeyword("FROM")
	stmt.Table = p.parseQualifiedName(false)
	stmt.Stop = spanFromNode(stmt.Table, stmt.Stop)

	if p.consumeKeyword("WHERE") {
		stmt.Where = p.parseExpr()
		stmt.Stop = spanFromNode(stmt.Where, stmt.Stop)
	}

	return stmt
}

func (p *Parser) parseCreate() Node {
	createTok := p.consumeSignificant()
	if !p.consumeKeyword("TABLE") {
		tok := p.peekSignificant()
		p.addError(tok, fmt.Sprintf("expected TABLE after CREATE, found %s", describeToken(tok)))
		return nil
	}

	return p.parseCreateTable(createTok)
}

func (p *Parser) parseCreateTable(createTok token.Token) *CreateTableStmt {
	stmt := &CreateTableStmt{
		Span: token.Span{
			Start: createTok.Pos(),
			Stop:  p.lastConsumed().End(),
		},
	}

	stmt.Name = p.parseQualifiedName(false)
	stmt.Stop = spanFromNode(stmt.Name, stmt.Stop)

	open := p.expectPunctuation("(")
	stmt.Stop = spanEnd(open, stmt.Stop)

	if p.matchPunctuation(")") {
		p.addError(p.peekSignificant(), fmt.Sprintf("expected column definition or table constraint, found %s", describeToken(p.peekSignificant())))
	}

	for !p.matchPunctuation(")") && p.peekSignificant().Kind != token.KindEOF {
		before := p.index

		if p.startsTableConstraint() {
			constraint := p.parseTableConstraint()
			if constraint != nil {
				stmt.Constraints = append(stmt.Constraints, constraint)
				stmt.Stop = constraint.End()
			}
		} else {
			column := p.parseColumnDef()
			if column != nil {
				stmt.Columns = append(stmt.Columns, column)
				stmt.Stop = column.End()
			}
		}

		if p.consumePunctuation(",") {
			continue
		}

		if p.index == before {
			p.consumeSignificant()
		}
	}

	closeTok := p.expectPunctuation(")")
	stmt.Stop = spanEnd(closeTok, stmt.Stop)

	return stmt
}

func (p *Parser) parseColumnDef() *ColumnDef {
	name := p.parseIdentifierToken(false)
	if name == nil {
		return nil
	}

	def := &ColumnDef{
		Span: token.Span{
			Start: name.Pos(),
			Stop:  name.End(),
		},
		Name: name,
	}

	def.Type = p.parseTypeSpec()
	def.Stop = spanFromNode(def.Type, def.Stop)

	for {
		var constraintName *Identifier
		if p.consumeKeyword("CONSTRAINT") {
			constraintName = p.parseIdentifierToken(false)
			def.Stop = spanFromNode(constraintName, def.Stop)
			if constraintName != nil && !p.startsColumnConstraint() {
				tok := p.peekSignificant()
				p.addError(tok, fmt.Sprintf("expected column constraint, found %s", describeToken(tok)))
			}
		}

		switch {
		case p.consumeKeyword("DEFAULT"):
			def.Default = p.parseExpr()
			def.Stop = spanFromNode(def.Default, def.Stop)
		case p.consumeKeyword("NULL"):
			nullTok := p.lastConsumed()
			constraint := &ConstraintDef{
				Span: token.Span{
					Start: constraintStart(constraintName, nullTok),
					Stop:  nullTok.End(),
				},
				Name: constraintName,
				Kind: ConstraintKindNull,
			}
			def.Constraints = append(def.Constraints, constraint)
			def.Stop = constraint.End()
		case p.consumeKeyword("NOT"):
			notTok := p.lastConsumed()
			nullTok := p.expectKeyword("NULL")
			constraint := &ConstraintDef{
				Span: token.Span{
					Start: constraintStart(constraintName, notTok),
					Stop:  spanEnd(nullTok, notTok.End()),
				},
				Name: constraintName,
				Kind: ConstraintKindNotNull,
			}
			def.Constraints = append(def.Constraints, constraint)
			def.Stop = constraint.End()
		case p.consumeKeyword("UNIQUE"):
			uniqueTok := p.lastConsumed()
			constraint := &ConstraintDef{
				Span: token.Span{
					Start: constraintStart(constraintName, uniqueTok),
					Stop:  uniqueTok.End(),
				},
				Name: constraintName,
				Kind: ConstraintKindUnique,
			}
			def.Constraints = append(def.Constraints, constraint)
			def.Stop = constraint.End()
		case p.consumeKeyword("PRIMARY"):
			primaryTok := p.lastConsumed()
			keyTok := p.expectKeyword("KEY")
			constraint := &ConstraintDef{
				Span: token.Span{
					Start: constraintStart(constraintName, primaryTok),
					Stop:  spanEnd(keyTok, primaryTok.End()),
				},
				Name: constraintName,
				Kind: ConstraintKindPrimaryKey,
			}
			def.Constraints = append(def.Constraints, constraint)
			def.Stop = constraint.End()
		case p.consumeKeyword("CHECK"):
			checkTok := p.lastConsumed()
			check, stop := p.parseParenthesizedExpr("CHECK constraint")
			constraint := &ConstraintDef{
				Span: token.Span{
					Start: constraintStart(constraintName, checkTok),
					Stop:  stop,
				},
				Name:  constraintName,
				Kind:  ConstraintKindCheck,
				Check: check,
			}
			def.Constraints = append(def.Constraints, constraint)
			def.Stop = constraint.End()
		case p.consumeKeyword("REFERENCES"):
			refTok := p.lastConsumed()
			reference := p.parseReferenceSpec(refTok.Pos())
			constraint := &ConstraintDef{
				Span: token.Span{
					Start: constraintStart(constraintName, refTok),
					Stop:  spanFromNode(reference, refTok.End()),
				},
				Name:      constraintName,
				Kind:      ConstraintKindReferences,
				Reference: reference,
			}
			def.Constraints = append(def.Constraints, constraint)
			def.Stop = constraint.End()
		default:
			return def
		}
	}
}

func (p *Parser) startsTableConstraint() bool {
	tok := p.peekSignificant()
	return tok.IsKeyword("CONSTRAINT") || tok.IsKeyword("CHECK") || tok.IsKeyword("UNIQUE") || tok.IsKeyword("PRIMARY") || tok.IsKeyword("FOREIGN")
}

func (p *Parser) startsColumnConstraint() bool {
	tok := p.peekSignificant()
	return tok.IsKeyword("NULL") || tok.IsKeyword("NOT") || tok.IsKeyword("UNIQUE") || tok.IsKeyword("PRIMARY") || tok.IsKeyword("CHECK") || tok.IsKeyword("REFERENCES")
}

func (p *Parser) parseTableConstraint() *ConstraintDef {
	var constraintName *Identifier
	if p.consumeKeyword("CONSTRAINT") {
		constraintName = p.parseIdentifierToken(false)
	}

	switch {
	case p.consumeKeyword("CHECK"):
		checkTok := p.lastConsumed()
		check, stop := p.parseParenthesizedExpr("CHECK constraint")
		return &ConstraintDef{
			Span: token.Span{
				Start: constraintStart(constraintName, checkTok),
				Stop:  stop,
			},
			Name:  constraintName,
			Kind:  ConstraintKindCheck,
			Check: check,
		}
	case p.consumeKeyword("UNIQUE"):
		uniqueTok := p.lastConsumed()
		columns, stop := p.parseParenthesizedIdentifierList("UNIQUE constraint")
		return &ConstraintDef{
			Span: token.Span{
				Start: constraintStart(constraintName, uniqueTok),
				Stop:  stop,
			},
			Name:    constraintName,
			Kind:    ConstraintKindUnique,
			Columns: columns,
		}
	case p.consumeKeyword("PRIMARY"):
		primaryTok := p.lastConsumed()
		p.expectKeyword("KEY")
		columns, stop := p.parseParenthesizedIdentifierList("PRIMARY KEY constraint")
		return &ConstraintDef{
			Span: token.Span{
				Start: constraintStart(constraintName, primaryTok),
				Stop:  stop,
			},
			Name:    constraintName,
			Kind:    ConstraintKindPrimaryKey,
			Columns: columns,
		}
	case p.consumeKeyword("FOREIGN"):
		foreignTok := p.lastConsumed()
		p.expectKeyword("KEY")
		columns, stop := p.parseParenthesizedIdentifierList("FOREIGN KEY column list")
		p.expectKeyword("REFERENCES")
		reference := p.parseReferenceSpec(foreignTok.End())
		return &ConstraintDef{
			Span: token.Span{
				Start: constraintStart(constraintName, foreignTok),
				Stop:  spanFromNode(reference, stop),
			},
			Name:      constraintName,
			Kind:      ConstraintKindForeignKey,
			Columns:   columns,
			Reference: reference,
		}
	default:
		tok := p.peekSignificant()
		p.addError(tok, fmt.Sprintf("expected table constraint, found %s", describeToken(tok)))
		return nil
	}
}

func (p *Parser) parseReferenceSpec(start token.Pos) *ReferenceSpec {
	reference := &ReferenceSpec{
		Span: token.Span{
			Start: start,
			Stop:  start,
		},
	}

	reference.Table = p.parseQualifiedName(false)
	reference.Stop = spanFromNode(reference.Table, reference.Stop)

	if p.matchPunctuation("(") {
		columns, stop := p.parseParenthesizedIdentifierList("REFERENCES column list")
		reference.Columns = columns
		reference.Stop = stop
	}

	return reference
}

func (p *Parser) parseDrop() Node {
	dropTok := p.consumeSignificant()
	if !p.consumeKeyword("TABLE") {
		tok := p.peekSignificant()
		p.addError(tok, fmt.Sprintf("expected TABLE after DROP, found %s", describeToken(tok)))
		return nil
	}

	stmt := &DropTableStmt{
		Span: token.Span{
			Start: dropTok.Pos(),
			Stop:  p.lastConsumed().End(),
		},
	}

	stmt.Name = p.parseQualifiedName(false)
	stmt.Stop = spanFromNode(stmt.Name, stmt.Stop)

	return stmt
}

func (p *Parser) parseExpr() Node {
	return p.parseOr()
}

func (p *Parser) parseOr() Node {
	left := p.parseAnd()
	for p.consumeKeyword("OR") {
		operator := p.lastConsumed()
		right := p.parseAnd()
		left = makeBinaryExpr(left, operator, right)
	}

	return left
}

func (p *Parser) parseAnd() Node {
	left := p.parseNot()
	for p.consumeKeyword("AND") {
		operator := p.lastConsumed()
		right := p.parseNot()
		left = makeBinaryExpr(left, operator, right)
	}

	return left
}

func (p *Parser) parseNot() Node {
	if !p.consumeKeyword("NOT") {
		return p.parsePredicate()
	}

	operator := p.lastConsumed()
	operand := p.parseNot()

	return makeUnaryExpr(operator, operand)
}

func (p *Parser) parsePredicate() Node {
	left := p.parseAdditive()
	for {
		switch {
		case p.matchComparisonOperator():
			operator := p.consumeSignificant()
			right := p.parseAdditive()
			left = makeBinaryExpr(left, operator, right)
		case p.consumeKeyword("IS"):
			left = p.finishIsExpr(left)
		case p.consumeKeyword("BETWEEN"):
			left = p.finishBetweenExpr(left, false)
		case p.consumeKeyword("IN"):
			left = p.finishInExpr(left, false)
		case p.consumeKeyword("LIKE"):
			left = p.finishLikeExpr(left, false)
		case p.matchKeyword("NOT") && p.peekSignificantN(1).IsKeyword("BETWEEN"):
			p.consumeKeyword("NOT")
			p.consumeKeyword("BETWEEN")
			left = p.finishBetweenExpr(left, true)
		case p.matchKeyword("NOT") && p.peekSignificantN(1).IsKeyword("IN"):
			p.consumeKeyword("NOT")
			p.consumeKeyword("IN")
			left = p.finishInExpr(left, true)
		case p.matchKeyword("NOT") && p.peekSignificantN(1).IsKeyword("LIKE"):
			p.consumeKeyword("NOT")
			p.consumeKeyword("LIKE")
			left = p.finishLikeExpr(left, true)
		default:
			return left
		}
	}
}

func (p *Parser) parseAdditive() Node {
	left := p.parseTerm()
	for p.matchOperator("+", "-") {
		operator := p.consumeSignificant()
		right := p.parseTerm()
		left = makeBinaryExpr(left, operator, right)
	}

	return left
}

func (p *Parser) parseTerm() Node {
	left := p.parseUnary()
	for p.matchOperator("*", "/", "%", "||") {
		operator := p.consumeSignificant()
		right := p.parseUnary()
		left = makeBinaryExpr(left, operator, right)
	}

	return left
}

func (p *Parser) parseUnary() Node {
	if p.matchOperator("+", "-") {
		operator := p.consumeSignificant()
		return makeUnaryExpr(operator, p.parseUnary())
	}

	return p.parsePrimary()
}

func (p *Parser) parsePrimary() Node {
	tok := p.peekSignificant()
	switch tok.Kind {
	case token.KindError:
		p.consumeSignificant()
		message := tok.Message
		if message == "" {
			message = "syntax error"
		}
		p.addError(tok, message)
		return nil
	case token.KindInteger:
		p.consumeSignificant()
		return &IntegerLiteral{Span: tok.Span, Text: tok.Lexeme}
	case token.KindDecimal, token.KindScientific, token.KindHex:
		p.consumeSignificant()
		return &FloatLiteral{Span: tok.Span, Text: tok.Lexeme}
	case token.KindString:
		p.consumeSignificant()
		return &StringLiteral{Span: tok.Span, Value: tok.Text}
	case token.KindIdentifier, token.KindQuotedIdentifier:
		if p.canStartFunctionCall() {
			return p.parseFunctionCall()
		}
		return p.parseNameOrStar()
	case token.KindKeyword:
		switch {
		case tok.IsKeyword("TRUE"):
			p.consumeSignificant()
			return &BoolLiteral{Span: tok.Span, Value: true}
		case tok.IsKeyword("FALSE"):
			p.consumeSignificant()
			return &BoolLiteral{Span: tok.Span, Value: false}
		case tok.IsKeyword("NULL"):
			p.consumeSignificant()
			return &NullLiteral{Span: tok.Span}
		case tok.IsKeyword("CASE"):
			return p.parseCaseExpr()
		case tok.IsKeyword("CAST"):
			return p.parseCastExpr()
		case p.canStartFunctionCall():
			return p.parseFunctionCall()
		case tok.Keyword.Class == token.KeywordNonReserved:
			return p.parseNameOrStar()
		default:
			p.consumeSignificant()
			p.addError(tok, fmt.Sprintf("expected expression, found %s", describeToken(tok)))
			return nil
		}
	case token.KindOperator:
		switch tok.Lexeme {
		case "?":
			p.consumeSignificant()
			return &ParamLiteral{Span: tok.Span, Text: tok.Lexeme}
		case "*":
			p.consumeSignificant()
			p.addError(tok, "bare * is not a valid scalar expression")
			return nil
		default:
			p.consumeSignificant()
			p.addError(tok, fmt.Sprintf("expected expression, found operator %q", tok.Lexeme))
			return nil
		}
	case token.KindPunctuation:
		if tok.Lexeme != "(" {
			p.consumeSignificant()
			p.addError(tok, fmt.Sprintf("expected expression, found %s", describeToken(tok)))
			return nil
		}

		p.consumeSignificant()
		expr := p.parseExpr()
		p.expectPunctuation(")")

		return expr
	default:
		p.consumeSignificant()
		p.addError(tok, fmt.Sprintf("expected expression, found %s", describeToken(tok)))
		return nil
	}
}

func (p *Parser) parseFunctionCall() Node {
	name := p.parseQualifiedName(true)
	if name == nil {
		return nil
	}

	open := p.expectPunctuation("(")
	call := &FunctionCall{
		Name: name,
		Span: token.Span{
			Start: name.Pos(),
			Stop:  spanEnd(open, name.End()),
		},
	}

	if p.consumeKeyword("DISTINCT") {
		call.SetQuantifier = "DISTINCT"
	} else if p.consumeKeyword("ALL") {
		call.SetQuantifier = "ALL"
	}

	if p.matchPunctuation(")") {
		closeTok := p.consumeSignificant()
		call.Stop = closeTok.End()
		return call
	}

	if p.matchOperator("*") {
		star := p.consumeSignificant()
		call.Args = append(call.Args, &Star{Span: star.Span})
		if call.SetQuantifier != "" {
			p.addError(star, fmt.Sprintf("%s cannot be used with * in function call", call.SetQuantifier))
		}
		if !functionAllowsStar(name) {
			p.addError(star, "* is only valid in COUNT(*)")
		}
		closeTok := p.expectPunctuation(")")
		call.Stop = spanEnd(closeTok, star.End())
		return call
	}

	for {
		argument := p.parseExpr()
		if argument != nil {
			call.Args = append(call.Args, argument)
			call.Stop = argument.End()
		}

		if !p.consumePunctuation(",") {
			break
		}
	}

	closeTok := p.expectPunctuation(")")
	call.Stop = spanEnd(closeTok, call.Stop)

	return call
}

func (p *Parser) parseCaseExpr() Node {
	caseTok := p.consumeSignificant()
	expr := &CaseExpr{
		Span: token.Span{
			Start: caseTok.Pos(),
			Stop:  caseTok.End(),
		},
	}

	if !p.matchKeyword("WHEN") {
		expr.Operand = p.parseExpr()
		if expr.Operand != nil {
			expr.Stop = expr.Operand.End()
		}
	}

	for p.consumeKeyword("WHEN") {
		whenTok := p.lastConsumed()
		condition := p.parseExpr()
		p.expectKeyword("THEN")
		result := p.parseExpr()

		when := &WhenClause{
			Span: token.Span{
				Start: whenTok.Pos(),
				Stop:  spanFromNode(result, spanFromNode(condition, whenTok.End())),
			},
			Condition: condition,
			Result:    result,
		}
		expr.Whens = append(expr.Whens, when)
		expr.Stop = when.End()
	}

	if len(expr.Whens) == 0 {
		p.addError(p.peekSignificant(), "expected WHEN in CASE expression")
	}

	if p.consumeKeyword("ELSE") {
		expr.Else = p.parseExpr()
		if expr.Else != nil {
			expr.Stop = expr.Else.End()
		}
	}

	endTok := p.expectKeyword("END")
	expr.Stop = spanEnd(endTok, expr.Stop)

	return expr
}

func (p *Parser) parseCastExpr() Node {
	castTok := p.consumeSignificant()
	p.expectPunctuation("(")

	expr := p.parseExpr()
	p.expectKeyword("AS")
	typeName := p.parseTypeSpec()

	closeTok := p.expectPunctuation(")")

	cast := &CastExpr{
		Span: token.Span{
			Start: castTok.Pos(),
			Stop:  spanEnd(closeTok, spanFromNode(typeName, spanFromNode(expr, castTok.End()))),
		},
		Expr: expr,
		Type: typeName,
	}

	return cast
}

func (p *Parser) parseNameOrStar() Node {
	first := p.parseIdentifierToken(false)
	if first == nil {
		return nil
	}

	parts := []*Identifier{first}
	for p.consumePunctuation(".") {
		if p.matchOperator("*") {
			star := p.consumeSignificant()
			qualifier := makeQualifiedName(parts)

			return &Star{
				Span: token.Span{
					Start: qualifier.Pos(),
					Stop:  star.End(),
				},
				Qualifier: qualifier,
			}
		}

		part := p.parseIdentifierToken(false)
		if part == nil {
			return makeQualifiedName(parts)
		}
		parts = append(parts, part)
	}

	if len(parts) == 1 {
		return parts[0]
	}

	return makeQualifiedName(parts)
}

func (p *Parser) parseQualifiedName(allowReservedKeywords bool) *QualifiedName {
	first := p.parseIdentifierToken(allowReservedKeywords)
	if first == nil {
		return nil
	}

	parts := []*Identifier{first}
	for p.consumePunctuation(".") {
		part := p.parseIdentifierToken(allowReservedKeywords)
		if part == nil {
			return makeQualifiedName(parts)
		}
		parts = append(parts, part)
	}

	return makeQualifiedName(parts)
}

func (p *Parser) parseTypeName() *QualifiedName {
	first := p.parseTypeNameToken()
	if first == nil {
		return nil
	}

	parts := []*Identifier{first}
	for p.consumePunctuation(".") {
		part := p.parseIdentifierToken(false)
		if part == nil {
			return makeQualifiedName(parts)
		}
		parts = append(parts, part)
	}

	for canContinueTypeName(p.peekSignificant()) {
		part := p.parseTypeNameToken()
		if part == nil {
			break
		}
		parts = append(parts, part)
	}

	return makeQualifiedName(parts)
}

func (p *Parser) parseIdentifierToken(allowReservedKeywords bool) *Identifier {
	tok := p.peekSignificant()
	switch tok.Kind {
	case token.KindIdentifier:
		p.consumeSignificant()
		return &Identifier{
			Span:   tok.Span,
			Name:   strings.ToLower(tok.Lexeme),
			Quoted: false,
		}
	case token.KindQuotedIdentifier:
		p.consumeSignificant()
		return &Identifier{
			Span:   tok.Span,
			Name:   tok.Text,
			Quoted: true,
		}
	case token.KindKeyword:
		if !allowReservedKeywords && tok.Keyword.Class != token.KeywordNonReserved {
			p.addError(tok, fmt.Sprintf("expected identifier, found %s", describeToken(tok)))
			return nil
		}

		p.consumeSignificant()
		return &Identifier{
			Span:   tok.Span,
			Name:   strings.ToLower(tok.Lexeme),
			Quoted: false,
		}
	default:
		if !preserveIdentifierMismatch(tok) {
			p.consumeSignificant()
		}
		p.addError(tok, fmt.Sprintf("expected identifier, found %s", describeToken(tok)))
		return nil
	}
}

func (p *Parser) parseTypeNameToken() *Identifier {
	return p.parseIdentifierToken(true)
}

func (p *Parser) finishIsExpr(left Node) Node {
	expr := &IsExpr{
		Span: token.Span{
			Start: nodePos(left),
			Stop:  nodeEnd(left),
		},
		Expr: left,
	}

	if p.consumeKeyword("NOT") {
		expr.Negated = true
	}

	switch {
	case p.consumeKeyword("NULL"):
		expr.Predicate = "NULL"
	case p.consumeKeyword("TRUE"):
		expr.Predicate = "TRUE"
	case p.consumeKeyword("FALSE"):
		expr.Predicate = "FALSE"
	case p.consumeKeyword("UNKNOWN"):
		expr.Predicate = "UNKNOWN"
	case p.consumeKeyword("DISTINCT"):
		expr.Predicate = "DISTINCT FROM"
		p.expectKeyword("FROM")
		expr.Right = p.parseAdditive()
	case expr.Negated && p.consumeKeyword("DISTINCT"):
		expr.Predicate = "DISTINCT FROM"
		p.expectKeyword("FROM")
		expr.Right = p.parseAdditive()
	default:
		tok := p.peekSignificant()
		p.addError(tok, fmt.Sprintf("expected IS predicate, found %s", describeToken(tok)))
	}

	expr.Stop = spanFromNode(expr.Right, p.lastConsumed().End())

	return expr
}

func (p *Parser) finishBetweenExpr(left Node, negated bool) Node {
	lower := p.parseAdditive()
	p.expectKeyword("AND")
	upper := p.parseAdditive()

	return &BetweenExpr{
		Span: token.Span{
			Start: nodePos(left),
			Stop:  spanFromNode(upper, spanFromNode(lower, nodeEnd(left))),
		},
		Expr:    left,
		Lower:   lower,
		Upper:   upper,
		Negated: negated,
	}
}

func (p *Parser) finishInExpr(left Node, negated bool) Node {
	p.expectPunctuation("(")

	list := make([]Node, 0, 4)
	if !p.matchPunctuation(")") {
		for {
			item := p.parseExpr()
			if item != nil {
				list = append(list, item)
			}

			if !p.consumePunctuation(",") {
				break
			}
		}
	} else {
		p.addError(p.peekSignificant(), "expected expression in IN list")
	}

	closeTok := p.expectPunctuation(")")

	return &InExpr{
		Span: token.Span{
			Start: nodePos(left),
			Stop:  spanEnd(closeTok, nodeEnd(left)),
		},
		Expr:    left,
		List:    list,
		Negated: negated,
	}
}

func (p *Parser) finishLikeExpr(left Node, negated bool) Node {
	pattern := p.parseAdditive()

	expr := &LikeExpr{
		Span: token.Span{
			Start: nodePos(left),
			Stop:  spanFromNode(pattern, nodeEnd(left)),
		},
		Expr:    left,
		Pattern: pattern,
		Negated: negated,
	}

	if p.consumeKeyword("ESCAPE") {
		expr.Escape = p.parseAdditive()
		expr.Stop = spanFromNode(expr.Escape, expr.Stop)
	}

	return expr
}

func (p *Parser) parseOptionalAlias() *Identifier {
	tok := p.peekSignificant()
	if !canStartAlias(tok) {
		return nil
	}

	return p.parseIdentifierToken(false)
}

func (p *Parser) parseParenthesizedIdentifierList(context string) ([]*Identifier, token.Pos) {
	open := p.expectPunctuation("(")
	stop := open.End()

	items := make([]*Identifier, 0, 2)
	if p.matchPunctuation(")") {
		p.addError(p.peekSignificant(), fmt.Sprintf("expected identifier in %s, found %s", context, describeToken(p.peekSignificant())))
	} else {
		for {
			item := p.parseIdentifierToken(false)
			if item != nil {
				items = append(items, item)
				stop = item.End()
			}

			if !p.consumePunctuation(",") {
				break
			}
		}
	}

	closeTok := p.expectPunctuation(")")
	return items, spanEnd(closeTok, stop)
}

func (p *Parser) parseParenthesizedExprList(context string) ([]Node, token.Pos) {
	open := p.expectPunctuation("(")
	stop := open.End()

	items := make([]Node, 0, 2)
	if p.matchPunctuation(")") {
		p.addError(p.peekSignificant(), fmt.Sprintf("expected expression in %s, found %s", context, describeToken(p.peekSignificant())))
	} else {
		for {
			item := p.parseExpr()
			if item != nil {
				items = append(items, item)
				stop = item.End()
			}

			if !p.consumePunctuation(",") {
				break
			}
		}
	}

	closeTok := p.expectPunctuation(")")
	return items, spanEnd(closeTok, stop)
}

func (p *Parser) parseParenthesizedExpr(context string) (Node, token.Pos) {
	open := p.expectPunctuation("(")
	stop := open.End()

	var expr Node
	if p.matchPunctuation(")") {
		p.addError(p.peekSignificant(), fmt.Sprintf("expected expression in %s, found %s", context, describeToken(p.peekSignificant())))
	} else {
		expr = p.parseExpr()
		stop = spanFromNode(expr, stop)
	}

	closeTok := p.expectPunctuation(")")
	return expr, spanEnd(closeTok, stop)
}

func (p *Parser) parseTypeSpec() *TypeName {
	first := p.parseTypeNameToken()
	if first == nil {
		return nil
	}

	segments := []*Identifier{first}
	for p.consumePunctuation(".") {
		part := p.parseTypeNameToken()
		if part == nil {
			break
		}
		segments = append(segments, part)
	}

	names := segments
	var qualifier *QualifiedName
	if len(segments) > 1 {
		qualifier = makeQualifiedName(segments[:len(segments)-1])
		names = []*Identifier{segments[len(segments)-1]}
	}

	for canContinueTypeName(p.peekSignificant()) {
		part := p.parseTypeNameToken()
		if part == nil {
			break
		}
		names = append(names, part)
	}

	typeName := &TypeName{
		Span: token.Span{
			Start: segments[0].Pos(),
			Stop:  names[len(names)-1].End(),
		},
		Qualifier: qualifier,
		Names:     names,
	}

	if p.matchPunctuation("(") {
		args, stop := p.parseParenthesizedExprList("type argument list")
		typeName.Args = args
		typeName.Stop = stop
	}

	return typeName
}

func (p *Parser) matchKeyword(word string) bool {
	return p.peekSignificant().IsKeyword(word)
}

func (p *Parser) consumeKeyword(word string) bool {
	if !p.matchKeyword(word) {
		return false
	}

	p.consumeSignificant()

	return true
}

func (p *Parser) expectKeyword(word string) token.Token {
	if p.matchKeyword(word) {
		return p.consumeSignificant()
	}

	tok := p.peekSignificant()
	p.addError(tok, fmt.Sprintf("expected %s, found %s", word, describeToken(tok)))

	return token.Token{}
}

func (p *Parser) matchPunctuation(lexeme string) bool {
	tok := p.peekSignificant()
	return tok.Kind == token.KindPunctuation && tok.Lexeme == lexeme
}

func (p *Parser) consumePunctuation(lexeme string) bool {
	if !p.matchPunctuation(lexeme) {
		return false
	}

	p.consumeSignificant()

	return true
}

func (p *Parser) expectPunctuation(lexeme string) token.Token {
	if p.matchPunctuation(lexeme) {
		return p.consumeSignificant()
	}

	tok := p.peekSignificant()
	p.addError(tok, fmt.Sprintf("expected %q, found %s", lexeme, describeToken(tok)))

	return token.Token{}
}

func (p *Parser) expectOperator(lexeme string) token.Token {
	if p.matchOperator(lexeme) {
		return p.consumeSignificant()
	}

	tok := p.peekSignificant()
	p.addError(tok, fmt.Sprintf("expected operator %q, found %s", lexeme, describeToken(tok)))

	return token.Token{}
}

func (p *Parser) matchOperator(operators ...string) bool {
	tok := p.peekSignificant()
	if tok.Kind != token.KindOperator {
		return false
	}

	for _, operator := range operators {
		if tok.Lexeme == operator {
			return true
		}
	}

	return false
}

func (p *Parser) matchComparisonOperator() bool {
	tok := p.peekSignificant()
	if tok.Kind != token.KindOperator {
		return false
	}

	switch tok.Lexeme {
	case "=", "<", "<=", ">", ">=", "<>", "!=":
		return true
	default:
		return false
	}
}

func (p *Parser) canStartFunctionCall() bool {
	index := p.nextSignificantIndex(p.index)
	if index >= len(p.tokens) {
		return false
	}

	tok := p.tokens[index]
	if !canStartFunctionName(tok) {
		return false
	}

	index = p.nextSignificantIndex(index + 1)
	for index < len(p.tokens) {
		dot := p.tokens[index]
		if dot.Kind != token.KindPunctuation || dot.Lexeme != "." {
			break
		}

		index = p.nextSignificantIndex(index + 1)
		if index >= len(p.tokens) || !canStartFunctionName(p.tokens[index]) {
			return false
		}
		index = p.nextSignificantIndex(index + 1)
	}

	if index >= len(p.tokens) {
		return false
	}

	return p.tokens[index].Kind == token.KindPunctuation && p.tokens[index].Lexeme == "("
}

func (p *Parser) peekSignificant() token.Token {
	return p.peekSignificantN(0)
}

func (p *Parser) peekSignificantN(offset int) token.Token {
	index := p.nextSignificantIndex(p.index)
	for offset > 0 && index < len(p.tokens) {
		index = p.nextSignificantIndex(index + 1)
		offset--
	}

	if len(p.tokens) == 0 {
		return eofToken(nil)
	}
	if index >= len(p.tokens) {
		return p.tokens[len(p.tokens)-1]
	}

	return p.tokens[index]
}

func (p *Parser) consumeSignificant() token.Token {
	for {
		tok := p.Consume()
		if !tok.Kind.IsComment() {
			return tok
		}
	}
}

func (p *Parser) lastConsumed() token.Token {
	if p.index == 0 {
		return token.Token{}
	}

	index := p.index - 1
	for index > 0 && p.tokens[index].Kind.IsComment() {
		index--
	}

	return p.tokens[index]
}

func (p *Parser) nextSignificantIndex(start int) int {
	index := start
	for index < len(p.tokens) && p.tokens[index].Kind.IsComment() {
		index++
	}

	return index
}

func eofToken(tokens []token.Token) token.Token {
	start := token.Pos{Line: 1, Column: 1, Offset: 0}
	if len(tokens) > 0 {
		start = tokens[len(tokens)-1].End()
		if start.IsZero() {
			start = token.Pos{Line: 1, Column: 1, Offset: 0}
		}
	}

	return token.Token{
		Kind: token.KindEOF,
		Span: token.Span{
			Start: start,
			Stop:  start,
		},
	}
}

func toDiagPosition(pos token.Pos) diag.Position {
	return diag.Position{
		Line:   pos.Line,
		Column: pos.Column,
		Offset: pos.Offset,
	}
}

func makeBinaryExpr(left Node, operator token.Token, right Node) *BinaryExpr {
	return &BinaryExpr{
		Span: token.Span{
			Start: nodePos(left),
			Stop:  spanFromNode(right, spanEnd(operator, nodeEnd(left))),
		},
		Operator: operator.Lexeme,
		Left:     left,
		Right:    right,
	}
}

func makeUnaryExpr(operator token.Token, operand Node) *UnaryExpr {
	return &UnaryExpr{
		Span: token.Span{
			Start: operator.Pos(),
			Stop:  spanFromNode(operand, operator.End()),
		},
		Operator: strings.ToUpper(operator.Lexeme),
		Operand:  operand,
	}
}

func makeQualifiedName(parts []*Identifier) *QualifiedName {
	if len(parts) == 0 {
		return nil
	}

	return &QualifiedName{
		Span: token.Span{
			Start: parts[0].Pos(),
			Stop:  parts[len(parts)-1].End(),
		},
		Parts: parts,
	}
}

func canStartFunctionName(tok token.Token) bool {
	switch tok.Kind {
	case token.KindIdentifier, token.KindQuotedIdentifier:
		return true
	case token.KindKeyword:
		switch tok.Keyword.Word {
		case "AND", "AS", "BEGIN", "BETWEEN", "CASE", "CAST", "COMMIT", "ELSE", "END", "ESCAPE", "IN", "IS", "LIKE", "NOT", "OR", "ROLLBACK", "THEN", "WHEN":
			return false
		default:
			return true
		}
	default:
		return false
	}
}

func canContinueTypeName(tok token.Token) bool {
	if tok.Kind != token.KindKeyword {
		return false
	}

	switch tok.Keyword.Word {
	case "BIT", "CHAR", "CHARACTER", "DAY", "DOUBLE", "HOUR", "MINUTE", "MONTH", "NATIONAL", "PRECISION", "SECOND", "TIME", "TIMESTAMP", "VARYING", "WITH", "WITHOUT", "YEAR", "ZONE":
		return true
	default:
		return false
	}
}

func canStartAlias(tok token.Token) bool {
	switch tok.Kind {
	case token.KindIdentifier, token.KindQuotedIdentifier:
		return true
	case token.KindKeyword:
		return tok.Keyword.Class == token.KeywordNonReserved && !isAliasStopKeyword(tok.Keyword.Word)
	default:
		return false
	}
}

func isAliasStopKeyword(word string) bool {
	switch word {
	case "ASC", "BY", "CHECK", "CONSTRAINT", "CREATE", "CROSS", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "FOR", "FOREIGN", "FROM", "FULL", "GROUP", "HAVING", "INNER", "INSERT", "INTO", "JOIN", "LEFT", "NATURAL", "ON", "ORDER", "OUTER", "PRIMARY", "REFERENCES", "RIGHT", "SELECT", "SET", "TABLE", "UNIQUE", "UPDATE", "USING", "VALUES", "WHERE":
		return true
	default:
		return false
	}
}

func isSelectClauseBoundary(tok token.Token) bool {
	if tok.Kind == token.KindEOF {
		return true
	}
	if tok.Kind == token.KindPunctuation {
		switch tok.Lexeme {
		case ";", ")", ",":
			return true
		}
	}

	return tok.IsKeyword("FROM") || tok.IsKeyword("WHERE") || tok.IsKeyword("GROUP") || tok.IsKeyword("HAVING") || tok.IsKeyword("ORDER")
}

func isFromClauseBoundary(tok token.Token) bool {
	if tok.Kind == token.KindEOF {
		return true
	}
	if tok.Kind == token.KindPunctuation {
		switch tok.Lexeme {
		case ";", ")", ",":
			return true
		}
	}

	return tok.IsKeyword("WHERE") || tok.IsKeyword("GROUP") || tok.IsKeyword("HAVING") || tok.IsKeyword("ORDER")
}

func isGroupByBoundary(tok token.Token) bool {
	if tok.Kind == token.KindEOF {
		return true
	}
	if tok.Kind == token.KindPunctuation {
		switch tok.Lexeme {
		case ";", ")", ",":
			return true
		}
	}

	return tok.IsKeyword("HAVING") || tok.IsKeyword("ORDER")
}

func isOrderByBoundary(tok token.Token) bool {
	if tok.Kind == token.KindEOF {
		return true
	}
	if tok.Kind == token.KindPunctuation {
		switch tok.Lexeme {
		case ";", ")", ",":
			return true
		}
	}

	return false
}

func describeToken(tok token.Token) string {
	switch tok.Kind {
	case token.KindEOF:
		return "end of input"
	case token.KindKeyword:
		return fmt.Sprintf("keyword %q", tok.Keyword.Word)
	case token.KindIdentifier:
		return fmt.Sprintf("identifier %q", tok.Lexeme)
	case token.KindQuotedIdentifier:
		return fmt.Sprintf("quoted identifier %q", tok.Text)
	case token.KindString:
		return fmt.Sprintf("string literal %q", tok.Text)
	case token.KindInteger, token.KindDecimal, token.KindScientific, token.KindHex:
		return fmt.Sprintf("numeric literal %q", tok.Lexeme)
	case token.KindOperator:
		return fmt.Sprintf("operator %q", tok.Lexeme)
	case token.KindPunctuation:
		return fmt.Sprintf("punctuation %q", tok.Lexeme)
	case token.KindError:
		if tok.Message != "" {
			return tok.Message
		}
		return "syntax error"
	default:
		if tok.Lexeme != "" {
			return fmt.Sprintf("%s %q", tok.Kind, tok.Lexeme)
		}
		return tok.Kind.String()
	}
}

func nodePos(node Node) token.Pos {
	if node == nil {
		return token.Pos{}
	}

	return node.Pos()
}

func nodeEnd(node Node) token.Pos {
	if node == nil {
		return token.Pos{}
	}

	return node.End()
}

func spanFromNode(node Node, fallback token.Pos) token.Pos {
	if node == nil {
		return fallback
	}

	return node.End()
}

func spanEnd(tok token.Token, fallback token.Pos) token.Pos {
	if tok.Kind == 0 && tok.Span.IsZero() {
		return fallback
	}

	return tok.End()
}

func preserveIdentifierMismatch(tok token.Token) bool {
	if tok.Kind == token.KindEOF {
		return true
	}
	if tok.Kind != token.KindPunctuation {
		return false
	}

	switch tok.Lexeme {
	case ";", ",", ")", "]":
		return true
	default:
		return false
	}
}

func functionAllowsStar(name *QualifiedName) bool {
	if name == nil || len(name.Parts) == 0 {
		return false
	}

	last := name.Parts[len(name.Parts)-1]
	if last.Quoted {
		return false
	}

	return strings.EqualFold(last.Name, "count")
}

func firstIdentifierPos(items []*Identifier) token.Pos {
	if len(items) == 0 {
		return token.Pos{}
	}

	return items[0].Pos()
}

func constraintStart(name *Identifier, tok token.Token) token.Pos {
	if name != nil {
		return name.Pos()
	}

	return tok.Pos()
}
