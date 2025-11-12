# VERMIG
> Version-based PostgreSQL auto-migration tool written in Go

```go
package migrations

import (
	"context"
	"log"
	
	"github.com/daarxwalker/vermig"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/**/*.sql
var migrations embed.FS

func main() {
    ctx := context.Background()
    config, parseUriErr := pgxpool.ParseConfig("<DB_URI>")
    if parseUriErr != nil {
        log.Fatalf("parse db uri failed: %s\n", parseUriErr)
    }
    db, connectErr := pgxpool.NewWithConfig(ctx, config)
    if connectErr != nil {
        log.Fatalf("db connect failed: %s\n", connectErr)
    }
    mg, createMigratorErr := vermig.New(
        ctx,
        vermig.WithDB(db),
        vermig.WithFS(migrations),
    )
    if createMigratorErr != nil {
        log.Fatalf("create migrator failed: %s\n", createMigratorErr)
    }
    if migrateErr := mg.Migrate(ctx, "1.0.0"); migrateErr != nil {
        log.Fatalf("migrate failed: %s\n", migrateErr)
    }
}

```