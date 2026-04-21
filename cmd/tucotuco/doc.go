// Package main provides the tucotuco command-line entry point.
//
// The command opens the default catalog file, currently tucotuco.catalog.json
// in the working directory, executes SQL scripts passed with --file, or starts
// an interactive REPL on stdin/stdout. Output is intended to be deterministic
// and machine-friendly. Phase 1 persists catalog metadata only; table rows
// remain in memory until the storage layer arrives in Phase 2.
package main
