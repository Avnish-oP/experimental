package db

import _ "embed"

// SchemaSQL is shared by database setup and isolated benchmark queues.
//go:embed schema.sql
var SchemaSQL string
