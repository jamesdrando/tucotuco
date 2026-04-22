package script

import (
	"errors"
	"reflect"
	"testing"

	"github.com/jamesdrando/tucotuco/pkg/embed"
)

type stubEngine struct {
	calls []string
	query func(string) (*embed.ResultSet, error)
	exec  func(string) (embed.CommandResult, error)
}

func (s *stubEngine) Query(sql string) (*embed.ResultSet, error) {
	s.calls = append(s.calls, "query:"+sql)
	if s.query != nil {
		return s.query(sql)
	}
	return &embed.ResultSet{}, nil
}

func (s *stubEngine) Exec(sql string) (embed.CommandResult, error) {
	s.calls = append(s.calls, "exec:"+sql)
	if s.exec != nil {
		return s.exec(sql)
	}
	return embed.CommandResult{}, nil
}

func TestSplitStatements(t *testing.T) {
	t.Parallel()

	got := SplitStatements(`
		-- leading comment;
		SELECT 'a; b', "c;d" FROM t; /* block; comment */
		INSERT INTO t VALUES ('x'';y');

		/* trailing comment */
	`)

	want := []string{
		`-- leading comment;
		SELECT 'a; b', "c;d" FROM t`,
		`/* block; comment */
		INSERT INTO t VALUES ('x'';y')`,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SplitStatements() mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestIsSelectStatement(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"SELECT 1":                       true,
		"  /* note */ SELECT 1":          true,
		"select 1":                       true,
		"INSERT INTO t VALUES (1)":       false,
		"-- comment\nUPDATE t SET a = 1": false,
		"/* comment */ DELETE FROM t":    false,
		"":                               false,
	}

	for input, want := range cases {
		input := input
		want := want
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			if got := IsSelectStatement(input); got != want {
				t.Fatalf("IsSelectStatement(%q) = %v, want %v", input, got, want)
			}
		})
	}
}

func TestRunnerRoutesAndReturnsStructuredResults(t *testing.T) {
	t.Parallel()

	engine := &stubEngine{
		query: func(_ string) (*embed.ResultSet, error) {
			return &embed.ResultSet{
				Columns: []embed.Column{{Name: "n", Type: "INTEGER"}},
				Rows:    [][]any{{int64(1)}},
			}, nil
		},
		exec: func(_ string) (embed.CommandResult, error) {
			return embed.CommandResult{RowsAffected: 3}, nil
		},
	}

	result, err := New(engine).Run("SELECT 1; INSERT INTO t VALUES (1);")
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	wantCalls := []string{
		"query:SELECT 1",
		"exec:INSERT INTO t VALUES (1)",
	}
	if !reflect.DeepEqual(engine.calls, wantCalls) {
		t.Fatalf("engine calls mismatch\n got: %#v\nwant: %#v", engine.calls, wantCalls)
	}

	if len(result.Statements) != 2 {
		t.Fatalf("Run() returned %d statements, want 2", len(result.Statements))
	}
	if got := result.Statements[0].Kind; got != StatementKindQuery {
		t.Fatalf("first statement kind = %q, want %q", got, StatementKindQuery)
	}
	if got := result.Statements[0].Query; got == nil || len(got.Rows) != 1 || len(got.Columns) != 1 {
		t.Fatalf("first statement query result not populated: %#v", got)
	}
	if got := result.Statements[1].Kind; got != StatementKindCommand {
		t.Fatalf("second statement kind = %q, want %q", got, StatementKindCommand)
	}
	if got := result.Statements[1].Command; got == nil || got.RowsAffected != 3 {
		t.Fatalf("second statement command result = %#v, want rows affected 3", got)
	}
}

func TestRunnerStopsAtFirstError(t *testing.T) {
	t.Parallel()

	engine := &stubEngine{
		query: func(_ string) (*embed.ResultSet, error) {
			return nil, errors.New("boom")
		},
	}

	result, err := New(engine).Run("SELECT 1; INSERT INTO t VALUES (1);")
	if err == nil {
		t.Fatal("Run() error = nil, want error")
	}

	if len(result.Statements) != 1 {
		t.Fatalf("Run() returned %d statements, want 1", len(result.Statements))
	}
	if got := result.Statements[0].Error; got != "boom" {
		t.Fatalf("statement error = %q, want %q", got, "boom")
	}
}
