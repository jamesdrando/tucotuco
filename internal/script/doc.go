// Package script provides a small, reusable SQL script runner.
//
// It centralizes the Phase 1 semicolon splitting and SELECT-versus-Exec
// dispatch behavior behind a tiny engine interface so callers can render the
// structured results however they need.
package script
