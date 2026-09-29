// Package migrations embeds the catalog service's SQL migrations.
//
// They are embedded rather than read from disk so that the binary is
// self-contained: a container built from a distroless base has no filesystem to
// read them from, and a deployment that can drift from its migrations is a
// deployment whose schema depends on which image tag was rolled out.
package migrations

import "embed"

// FS holds the migration files, applied in filename order.
//
//go:embed *.sql
var FS embed.FS
