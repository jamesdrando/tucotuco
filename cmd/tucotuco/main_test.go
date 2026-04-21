package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunFileExecutesScript(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "script.sql")
	if err := os.WriteFile(scriptPath, []byte("SELECT 1;\n"), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}

	code, stdout, stderr := invokeRun(t, []string{"--file", scriptPath}, "")
	if code != 0 {
		t.Fatalf("run returned exit code %d, stderr=%q", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("expected no stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "1") {
		t.Fatalf("expected script output to contain 1, got %q", stdout)
	}
}

func TestRunFileDashExecutesScriptFromStdin(t *testing.T) {
	code, stdout, stderr := invokeRun(t, []string{"--file", "-"}, "SELECT 1;\n")
	if code != 0 {
		t.Fatalf("run returned exit code %d, stderr=%q", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("expected no stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "1") {
		t.Fatalf("expected stdin script output to contain 1, got %q", stdout)
	}
}

func TestRunREPLExecutesQueryFromStdin(t *testing.T) {
	code, stdout, stderr := invokeRun(t, nil, "SELECT 1;\n")
	if code != 0 {
		t.Fatalf("run returned exit code %d, stderr=%q", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("expected no stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "1") {
		t.Fatalf("expected repl output to contain 1, got %q", stdout)
	}
}

func TestRunReturnsNonZeroForSQLError(t *testing.T) {
	code, _, stderr := invokeRun(t, nil, "SELCT 1;\n")
	if code == 0 {
		t.Fatal("expected non-zero exit code for SQL error")
	}
	if stderr == "" {
		t.Fatal("expected stderr output for SQL error")
	}
}

func TestRunPreservesSuccessfulOutputBeforeFailure(t *testing.T) {
	code, stdout, stderr := invokeRun(t, nil, "SELECT 1;\nSELCT 1;\n")
	if code == 0 {
		t.Fatal("expected non-zero exit code for SQL error")
	}
	if stderr == "" {
		t.Fatal("expected stderr output for SQL error")
	}
	if !strings.Contains(stdout, "1") {
		t.Fatalf("expected successful output before failure, got %q", stdout)
	}
}

func TestRunReturnsNonZeroForMissingFile(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing.sql")
	code, _, stderr := invokeRun(t, []string{"--file", missingPath}, "")
	if code == 0 {
		t.Fatal("expected non-zero exit code for missing file")
	}
	if stderr == "" {
		t.Fatal("expected stderr output for missing file")
	}
}

func TestRunFileSplitsTopLevelSemicolonsOnly(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "script.sql")
	script := "SELECT 'semi;colon' AS label; -- comment with ;\nSELECT 1;\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}

	code, stdout, stderr := invokeRun(t, []string{"--file", scriptPath}, "")
	if code != 0 {
		t.Fatalf("run returned exit code %d, stderr=%q", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("expected no stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "semi;colon") {
		t.Fatalf("expected script output to contain semicolon string literal, got %q", stdout)
	}
	if !strings.Contains(stdout, "1") {
		t.Fatalf("expected script output to contain 1, got %q", stdout)
	}
}

func TestRunPersistsCatalogButNotRows(t *testing.T) {
	dir := t.TempDir()

	createAndInsert := "CREATE TABLE widgets (name TEXT);\nINSERT INTO widgets VALUES ('persist-me');\n"
	createCode, _, createStderr := invokeRunInDir(t, dir, []string{"--file", writeScriptFile(t, dir, createAndInsert)}, "")
	if createCode != 0 {
		t.Fatalf("setup run returned exit code %d, stderr=%q", createCode, createStderr)
	}
	if createStderr != "" {
		t.Fatalf("expected no stderr during setup, got %q", createStderr)
	}

	catalogPath := filepath.Join(dir, "tucotuco.catalog.json")
	if _, err := os.Stat(catalogPath); err != nil {
		t.Fatalf("expected catalog metadata file at %s: %v", catalogPath, err)
	}

	code, stdout, stderr := invokeRunInDir(t, dir, []string{"--file", writeScriptFile(t, dir, "SELECT * FROM widgets;\n")}, "")
	if code != 0 {
		t.Fatalf("run returned exit code %d, stderr=%q", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("expected no stderr, got %q", stderr)
	}
	if strings.Contains(stdout, "persist-me") {
		t.Fatalf("expected row data not to persist across reopen, got %q", stdout)
	}
}

func invokeRun(t *testing.T, args []string, stdin string) (int, string, string) {
	t.Helper()

	return invokeRunInDir(t, t.TempDir(), args, stdin)
}

func invokeRunInDir(t *testing.T, workdir string, args []string, stdin string) (int, string, string) {
	t.Helper()

	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	if err := os.Chdir(workdir); err != nil {
		t.Fatalf("chdir workdir: %v", err)
	}
	defer func() {
		_ = os.Chdir(oldwd)
	}()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run(args, strings.NewReader(stdin), &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func writeScriptFile(t *testing.T, dir, content string) string {
	t.Helper()

	path := filepath.Join(dir, "script.sql")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}
