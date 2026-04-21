// Package driver integrates tucotuco with database/sql.
//
// It registers the tucotuco driver name for sql.Open and routes calls through
// the current Phase 1 embed engine. Catalog metadata persists across opens, but
// row storage remains in memory until Phase 2 storage lands.
package driver
