package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/script"
	"github.com/jamesdrando/tucotuco/pkg/embed"
)

const defaultCatalogPath = "tucotuco.catalog.json"

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if stdin == nil {
		stdin = strings.NewReader("")
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}

	fs := flag.NewFlagSet("tucotuco", flag.ContinueOnError)
	fs.SetOutput(stderr)

	dbPath := fs.String("db", defaultCatalogPath, "catalog path")
	filePath := fs.String("file", "", "SQL script path or - for stdin")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: tucotuco [--db path] [--file path|-]\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.Usage()
			return 0
		}
		fs.Usage()
		fmt.Fprintln(stderr, err)
		return 2
	}

	db, err := embed.Open(*dbPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	runner := script.New(db)
	if *filePath != "" {
		return runScriptSource(runner, *filePath, stdin, stdout, stderr)
	}

	return runRepl(runner, stdin, stdout, stderr)
}

func runScriptSource(runner *script.Runner, path string, stdin io.Reader, stdout, stderr io.Writer) int {
	var content []byte
	var err error

	if path == "-" {
		content, err = io.ReadAll(stdin)
	} else {
		content, err = os.ReadFile(path)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	return runSQLText(runner, string(content), stdout, stderr)
}

func runRepl(runner *script.Runner, stdin io.Reader, stdout, stderr io.Writer) int {
	reader := bufio.NewReader(stdin)
	for {
		line, err := reader.ReadString('\n')
		if len(line) != 0 {
			if code := runSQLText(runner, line, stdout, stderr); code != 0 {
				return code
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0
			}
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
}

func runSQLText(runner *script.Runner, text string, stdout, stderr io.Writer) int {
	result, err := runner.Run(text)
	return renderScriptRun(result, err, stdout, stderr)
}
