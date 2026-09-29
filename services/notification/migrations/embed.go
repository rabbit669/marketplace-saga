// Package migrations embeds the notification service's SQL migrations.
package migrations

import "embed"

// FS holds the migration files, applied in filename order.
//
//go:embed *.sql
var FS embed.FS
