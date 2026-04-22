package driver

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jamesdrando/tucotuco/pkg/embed"
)

const driverName = "tucotuco"

var globalDriver = &tucotucoDriver{}

func init() {
	sql.Register(driverName, globalDriver)
}

type tucotucoDriver struct{}

func (d *tucotucoDriver) Open(name string) (sqldriver.Conn, error) {
	return (&dsnConnector{dsn: name}).Connect(context.Background())
}

func (d *tucotucoDriver) OpenConnector(name string) (sqldriver.Connector, error) {
	return &dsnConnector{dsn: name}, nil
}

type dsnConnector struct {
	dsn  string
	once sync.Once
	db   *embed.DB
	err  error
}

func (c *dsnConnector) Connect(ctx context.Context) (sqldriver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	db, err := c.open()
	if err != nil {
		return nil, err
	}

	return &conn{db: db}, nil
}

func (c *dsnConnector) Driver() sqldriver.Driver {
	return globalDriver
}

func (c *dsnConnector) open() (*embed.DB, error) {
	c.once.Do(func() {
		c.db, c.err = embed.Open(c.dsn)
	})

	return c.db, c.err
}

type conn struct {
	mu sync.Mutex
	db *embed.DB
	tx *embed.Tx
}

func (c *conn) Prepare(query string) (sqldriver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

func (c *conn) Close() error {
	c.mu.Lock()
	c.tx = nil
	c.mu.Unlock()
	return nil
}

func (c *conn) Begin() (sqldriver.Tx, error) {
	return c.BeginTx(context.Background(), sqldriver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts sqldriver.TxOptions) (sqldriver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateTxOptions(opts); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tx != nil {
		return nil, featureNotSupported("nested transactions are not supported")
	}

	embeddedTx, err := c.db.Begin()
	if err != nil {
		return nil, err
	}
	c.tx = embeddedTx

	return &tx{conn: c, tx: embeddedTx}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	return c.execContext(ctx, query, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	return c.queryContext(ctx, query, args)
}

func (c *conn) Ping(ctx context.Context) error {
	return ctx.Err()
}

func (c *conn) execContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) != 0 {
		return nil, featureNotSupported("bind parameters are not supported")
	}

	tx := c.activeTx()
	if tx != nil {
		result, err := tx.Exec(query)
		if err != nil {
			return nil, err
		}

		return sqldriver.RowsAffected(result.RowsAffected), nil
	}

	result, err := c.db.Exec(query)
	if err != nil {
		return nil, err
	}

	return sqldriver.RowsAffected(result.RowsAffected), nil
}

func (c *conn) queryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) != 0 {
		return nil, featureNotSupported("bind parameters are not supported")
	}

	tx := c.activeTx()
	if tx != nil {
		result, err := tx.Query(query)
		if err != nil {
			return nil, err
		}

		return newRows(result)
	}

	result, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}

	return newRows(result)
}

func (c *conn) activeTx() *embed.Tx {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.tx
}

func (c *conn) clearTx(tx *embed.Tx) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tx == tx {
		c.tx = nil
	}
}

type tx struct {
	conn *conn
	tx   *embed.Tx
}

func (t *tx) Commit() error {
	err := t.tx.Commit()
	t.finish()
	return err
}

func (t *tx) Rollback() error {
	err := t.tx.Rollback()
	t.finish()
	return err
}

func (t *tx) finish() {
	if t == nil || t.conn == nil || t.tx == nil {
		return
	}

	t.conn.clearTx(t.tx)
	t.conn = nil
	t.tx = nil
}

type stmt struct {
	conn  *conn
	query string
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return 0
}

func (s *stmt) Exec(args []sqldriver.Value) (sqldriver.Result, error) {
	if len(args) != 0 {
		return nil, featureNotSupported("bind parameters are not supported")
	}

	return s.conn.execContext(context.Background(), s.query, nil)
}

func (s *stmt) Query(args []sqldriver.Value) (sqldriver.Rows, error) {
	if len(args) != 0 {
		return nil, featureNotSupported("bind parameters are not supported")
	}

	return s.conn.queryContext(context.Background(), s.query, nil)
}

func (s *stmt) ExecContext(ctx context.Context, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	return s.conn.execContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	return s.conn.queryContext(ctx, s.query, args)
}

type rows struct {
	columns []embed.Column
	data    [][]sqldriver.Value
	index   int
}

func newRows(result *embed.ResultSet) (*rows, error) {
	out := &rows{}
	if result == nil {
		return out, nil
	}

	out.columns = append([]embed.Column(nil), result.Columns...)
	if len(result.Rows) == 0 {
		return out, nil
	}

	out.data = make([][]sqldriver.Value, len(result.Rows))
	for rowIndex, row := range result.Rows {
		out.data[rowIndex] = make([]sqldriver.Value, len(row))
		for colIndex, value := range row {
			driverValue, err := toDriverValue(value)
			if err != nil {
				return nil, err
			}
			out.data[rowIndex][colIndex] = driverValue
		}
	}

	return out, nil
}

func (r *rows) Columns() []string {
	if r == nil || len(r.columns) == 0 {
		return nil
	}

	columns := make([]string, len(r.columns))
	for index, column := range r.columns {
		columns[index] = column.Name
	}

	return columns
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []sqldriver.Value) error {
	if r == nil || r.index >= len(r.data) {
		return io.EOF
	}

	row := r.data[r.index]
	r.index++
	for index := range dest {
		if index < len(row) {
			dest[index] = row[index]
			continue
		}
		dest[index] = nil
	}

	return nil
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if r == nil || index < 0 || index >= len(r.columns) {
		return ""
	}

	return r.columns[index].Type
}

func (r *rows) ColumnTypeNullable(int) (bool, bool) {
	return false, false
}

func (r *rows) ColumnTypeLength(index int) (int64, bool) {
	typ := strings.ToUpper(r.ColumnTypeDatabaseTypeName(index))
	if typ == "" {
		return 0, false
	}

	_, length, ok := parseSingleLengthType(typ)
	return length, ok
}

func (r *rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	typ := strings.ToUpper(r.ColumnTypeDatabaseTypeName(index))
	if typ == "" {
		return 0, 0, false
	}

	precision, scale, ok := parsePrecisionScaleType(typ)
	return precision, scale, ok
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	typ := strings.ToUpper(r.ColumnTypeDatabaseTypeName(index))
	switch {
	case typ == "":
		return reflect.TypeOf((*any)(nil)).Elem()
	case strings.Contains(typ, "BOOL"):
		return reflect.TypeOf(true)
	case strings.Contains(typ, "INT"), strings.Contains(typ, "SERIAL"):
		return reflect.TypeOf(int64(0))
	case strings.Contains(typ, "REAL"), strings.Contains(typ, "DOUBLE"), strings.Contains(typ, "FLOAT"):
		return reflect.TypeOf(float64(0))
	case strings.Contains(typ, "CHAR"), strings.Contains(typ, "CLOB"), strings.Contains(typ, "TEXT"), strings.Contains(typ, "DECIMAL"), strings.Contains(typ, "NUMERIC"):
		return reflect.TypeOf("")
	case strings.Contains(typ, "BINARY"), strings.Contains(typ, "BLOB"), strings.Contains(typ, "JSON"):
		return reflect.TypeOf([]byte(nil))
	case strings.Contains(typ, "DATE"), strings.Contains(typ, "TIMESTAMP"), strings.Contains(typ, "TIME"):
		if strings.Contains(typ, "TIME") && !strings.Contains(typ, "STAMP") {
			return reflect.TypeOf(time.Duration(0))
		}
		return reflect.TypeOf(time.Time{})
	default:
		if sample := r.sampleValue(index); sample != nil {
			return reflect.TypeOf(sample)
		}
		return reflect.TypeOf((*any)(nil)).Elem()
	}
}

func (r *rows) sampleValue(index int) any {
	if r == nil || index < 0 {
		return nil
	}

	for _, row := range r.data {
		if index >= len(row) {
			continue
		}
		if row[index] != nil {
			return row[index]
		}
	}

	return nil
}

func validateTxOptions(opts sqldriver.TxOptions) error {
	var issues []string
	if opts.ReadOnly {
		issues = append(issues, "read-only transactions")
	}
	if opts.Isolation != sqldriver.IsolationLevel(sql.LevelDefault) {
		issues = append(issues, fmt.Sprintf("isolation level %s", isolationLevelName(opts.Isolation)))
	}
	if len(issues) == 0 {
		return nil
	}

	return featureNotSupported("transaction options are not supported: " + strings.Join(issues, ", "))
}

func isolationLevelName(level sqldriver.IsolationLevel) string {
	switch level {
	case sqldriver.IsolationLevel(sql.LevelDefault):
		return "default"
	case sqldriver.IsolationLevel(sql.LevelReadUncommitted):
		return "read uncommitted"
	case sqldriver.IsolationLevel(sql.LevelReadCommitted):
		return "read committed"
	case sqldriver.IsolationLevel(sql.LevelWriteCommitted):
		return "write committed"
	case sqldriver.IsolationLevel(sql.LevelRepeatableRead):
		return "repeatable read"
	case sqldriver.IsolationLevel(sql.LevelSnapshot):
		return "snapshot"
	case sqldriver.IsolationLevel(sql.LevelSerializable):
		return "serializable"
	case sqldriver.IsolationLevel(sql.LevelLinearizable):
		return "linearizable"
	default:
		return fmt.Sprintf("%d", level)
	}
}

func featureNotSupported(message string) error {
	return &embed.SQLError{
		Diagnostics: []embed.Diagnostic{
			{
				Severity: "ERROR",
				SQLState: "0A000",
				Message:  message,
			},
		},
	}
}

func toDriverValue(value any) (sqldriver.Value, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return value, nil
	case string:
		return value, nil
	case []byte:
		return append([]byte(nil), value...), nil
	case time.Time:
		return value, nil
	case time.Duration:
		return nil, featureNotSupported(fmt.Sprintf("unsupported result cell type %T", value))
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case uint:
		return int64(value), nil
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		if value > ^uint64(0)>>1 {
			return nil, featureNotSupported(fmt.Sprintf("unsupported result cell type %T", value))
		}
		return int64(value), nil
	case float32:
		return float64(value), nil
	case float64:
		return value, nil
	default:
		return nil, featureNotSupported(fmt.Sprintf("unsupported result cell type %T", value))
	}
}

func parseSingleLengthType(typ string) (string, int64, bool) {
	open := strings.IndexByte(typ, '(')
	closeIndex := strings.LastIndexByte(typ, ')')
	if open < 0 || closeIndex < 0 || closeIndex <= open+1 {
		return "", 0, false
	}

	var base = strings.TrimSpace(typ[:open])
	content := strings.TrimSpace(typ[open+1 : closeIndex])
	if strings.Contains(content, ",") {
		return base, 0, false
	}

	var length int64
	_, err := fmt.Sscan(content, &length)
	if err != nil {
		return base, 0, false
	}

	return base, length, true
}

func parsePrecisionScaleType(typ string) (int64, int64, bool) {
	open := strings.IndexByte(typ, '(')
	closeIndex := strings.LastIndexByte(typ, ')')
	if open < 0 || closeIndex < 0 || closeIndex <= open+1 {
		return 0, 0, false
	}

	content := strings.TrimSpace(typ[open+1 : closeIndex])
	parts := strings.Split(content, ",")
	if len(parts) != 2 {
		return 0, 0, false
	}

	var precision, scale int64
	if _, err := fmt.Sscan(strings.TrimSpace(parts[0]), &precision); err != nil {
		return 0, 0, false
	}
	if _, err := fmt.Sscan(strings.TrimSpace(parts[1]), &scale); err != nil {
		return 0, 0, false
	}

	return precision, scale, true
}

var (
	_ sqldriver.DriverContext                  = (*tucotucoDriver)(nil)
	_ sqldriver.Conn                           = (*conn)(nil)
	_ sqldriver.ConnBeginTx                    = (*conn)(nil)
	_ sqldriver.ExecerContext                  = (*conn)(nil)
	_ sqldriver.QueryerContext                 = (*conn)(nil)
	_ sqldriver.Pinger                         = (*conn)(nil)
	_ sqldriver.Stmt                           = (*stmt)(nil)
	_ sqldriver.StmtExecContext                = (*stmt)(nil)
	_ sqldriver.StmtQueryContext               = (*stmt)(nil)
	_ sqldriver.Tx                             = (*tx)(nil)
	_ sqldriver.Rows                           = (*rows)(nil)
	_ sqldriver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ sqldriver.RowsColumnTypeNullable         = (*rows)(nil)
	_ sqldriver.RowsColumnTypeLength           = (*rows)(nil)
	_ sqldriver.RowsColumnTypePrecisionScale   = (*rows)(nil)
	_ sqldriver.RowsColumnTypeScanType         = (*rows)(nil)
)
